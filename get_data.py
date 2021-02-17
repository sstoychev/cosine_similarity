import sys
import MySQLdb
import datetime
from collections import namedtuple
from config import Config


class GetData(Config):
    """
    Class to get the data.
    This can be anything, just return dictionary of vectors and list of vector groups

    The format of vectors is:
    vectors={
        product_id: {user_id: coef, user_id2: coef2, user_id3: coef3}
        product_id2: {user_id: coef, user_id15: coef2, user_id100: coef3}
    }
    Keep only the !=0 values.

    The format of vector_groups is:
    vector_groups = [
        [product_id, product_id2, product_id3],
        [product_id4, product_id5, product_id6],
    ]
    The couples of products will be generated within each group. If you want you can simply create one group with all
    """

    def get_data(self):
        s = datetime.datetime.now()
        db = None
        vectors = {}
        cat_groups = []
        prod2cat = {}
        max_user_id = 0
        tree = {}
        try:
            db = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASS, db=self.PROD_DB)
            cur = db.cursor()
            # generate product groups base on category groups
            print('generate product groups based on category groups: ')
            cur.execute("""
                        SELECT
                            cat_id,
                            topcat_id
                        FROM
                            `td_categories`
                        WHERE
                            `language_id` = '1'
                            AND `topcat_id` IS NOT NULL
            """)

            row = namedtuple('row', 'cat_id topcat_id')
            for r in map(lambda x: row(*x), cur.fetchall()):
                tree[r.cat_id] = r.topcat_id

            children_direct = {}  # immediate children of category
            for cat, topcat in tree.items():
                children_direct[topcat] = children_direct.get(topcat, [])
                children_direct[topcat].append(cat)

            children_full = {}  # all children of category - including children of children
            for cat in tree:
                if current_children := self.get_children(cat, children_direct):
                    children_full[cat] = current_children

            cat_groups = [self.get_children(cat_id, children_direct) for cat_ids in self.CAT_GROUPS for cat_id in cat_ids]
            cats_str = ','.join([str(cat) for cat_group in cat_groups for cat in cat_group])

            print((datetime.datetime.now() - s).total_seconds(), 'secs')
            # generate {product: groups} dictionary
            print('generate products information dictionary: ')
            cur.execute(f"""
                        SELECT
                            `id`,
                            `cat_id`,
                            `viewed`
                        FROM
                            `td_articules`
                        WHERE
                            `language_id` = '1'
                            AND `approved` = 1
                            AND `deleted` = 0
                            AND `cat_id` IN ({cats_str})
            """)
            row1 = namedtuple('row1', 'id cat_id viewed')
            for r in map(lambda x: row1(*x), cur.fetchall()):
                prod2cat[r.id] = r.cat_id
            print((datetime.datetime.now() - s).total_seconds(), 'secs')
            # get users
            print('get users: ')
            cur.execute(f"""
                SELECT
                    MAX(`user_id`) as max_user_id
                FROM
                    `td_orders`
                WHERE
                    `user_id` != 0
                    AND `moderator_status` IN (1, 2, 4, 5, 6, 7, 10, 12, 13)
                    AND `cat_id` IN ({cats_str})
            """)
            row2 = namedtuple('row2', 'max_user_id')
            for r in map(lambda x: row2(*x), cur.fetchall()):
                max_user_id = r.max_user_id
            print((datetime.datetime.now() - s).total_seconds(), 'secs')
            print('max_user_id', max_user_id)
            # generate vectors
            print('generate vectors')
            print('get data from orders: ')
            cur.execute(f"""
                SELECT
                    `id`,
                    `articule_id`,
                    `user_id`,
                    `cat_id`
                FROM
                    `td_orders`
                WHERE
                    `user_id` != 0
                    AND `moderator_status` IN (1, 2, 4, 5, 6, 7, 10, 12, 13)
                    AND `cat_id`  IN ({cats_str})
            """)
            row3 = namedtuple('row3', 'id articule_id user_id cat_id')
            for r in map(lambda x: row3(*x), cur.fetchall()):
                if r.articule_id not in prod2cat:
                    continue
                if r.articule_id not in vectors:
                    vectors[r.articule_id] = {}

                if r.user_id not in vectors[r.articule_id]:
                    vectors[r.articule_id][r.user_id] = 0

                vectors[r.articule_id][r.user_id] = self.WEIGHTS['sale']  # no matter how many sales we count as one
            print((datetime.datetime.now() - s).total_seconds(), 'secs')
            print('get data from views: ')
            cur.execute(f"""
                SELECT
                    `id`,
                    `user_id`,
                    `param2`,
                    `cnt`
                FROM
                    `{self.SIM_DB}`.`td_user_visits_aggregated`
            """)
            row4 = namedtuple('row3', 'id user_id param2 cnt')
            for r in map(lambda x: row4(*x), cur.fetchall()):
                if (r.param2 not in prod2cat) or (r.user_id > max_user_id):
                    continue
                if r.param2 not in vectors:
                    vectors[r.param2] = {}

                if r.user_id not in vectors[r.param2]:
                    vectors[r.param2][r.user_id] = 0
                # in the table we have one record for user per product
                # so we add it once. If we add depending on number it should be += (r.cnt * config.WEIGHTS['view'])
                vectors[r.param2][r.user_id] += self.WEIGHTS['view']
            print((datetime.datetime.now() - s).total_seconds(), 'secs')
        except Exception as e:
            print(e)
            sys.exit(1001)
        finally:
            if db:
                db.close()
        # generate vector groups
        vector_groups = [[] for _ in cat_groups]
        for k in vectors.keys():
            for i, cat_group in enumerate(cat_groups):
                if (k in prod2cat) and (prod2cat[k] in cat_group):
                    vector_groups[i].append(k)

        vectors_len = len(vectors)
        vectors_max_len = max([len(v) for k, v in vectors.items()])
        vectors_all_items = sum([len(v) for k, v in vectors.items()])
        print('vectors len', vectors_len)
        print('vectors size', sys.getsizeof(vectors), 'bytes')
        # print('vectors real size', sum([sys.getsizeof(v)+sys.getsizeof(k) for k, v in vectors.items()]), 'bytes')
        print('vectors total elements', vectors_all_items)
        print('vectors average elements', vectors_all_items / vectors_len)
        print('longest vector', vectors_max_len)
        print('selects:', (datetime.datetime.now() - s).total_seconds(), 'secs')

        # calculate expected vectors and prepare
        expected = 0
        for i, vg in enumerate(vector_groups):
            vg_len = len(vg)
            expected += (vg_len * (vg_len - 1)) / 2
        print('expected calculations', expected)

        # prepare groups for further calculations

        return vectors, vector_groups, prod2cat, tree, children_full

    def get_children(self, topcat_id: int, tree: dict) -> list:
        current_children = []
        for child in tree.get(topcat_id, []):
            new_children = self.get_children(child, tree)
            if new_children:
                current_children.append(new_children)
        return tree.get(topcat_id, []) + self.flatten(current_children)

    def flatten(self, li) -> list:
        return sum(([x] if not isinstance(x, list) else self.flatten(x)
                    for x in li), [])
