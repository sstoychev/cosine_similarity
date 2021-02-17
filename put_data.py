import MySQLdb
from config import Config


class PutData(Config):

    def put_data(self, top_items: dict):
        print('process_top_items SQL', sum([len(v) for k, v in top_items.items()]), 'records')
        v_list = []
        db = None
        counter = 0
        try:
            db = MySQLdb.connect(host=self.HOST, user=self.USER, passwd=self.PASS, db=self.SIM_DB)
            cur = db.cursor()
            cur.execute("""TRUNCATE TABLE td_similarity""")
            for p1_relation, val in top_items.items():
                p1, relation = p1_relation
                for [p2, coef] in val:
                    v_list.append(f'({p1}, {p2}, {relation}, {coef})')
                    if len(v_list) == self.INSERT_CHUNK_SIZE:
                        sql = """
                            INSERT INTO
                                td_similarity
                            (`prod1`, `prod2`, `relation`, `coef`)
                            VALUES {}""".format(','.join(v_list))
                        # print(sql)
                        cur.execute(sql)
                        counter += 1
                        print(f'INSERT {counter * self.INSERT_CHUNK_SIZE}')
                        v_list = []
                        db.commit()
            if len(v_list) > 0:
                cur.execute("""
                    INSERT INTO
                        td_similarity
                    (`prod1`, `prod2`, `relation`, `coef`)
                    VALUES {}""".format(','.join(v_list)))
                db.commit()
        except Exception as e:
            print(e)
        finally:
            if db:
                db.close()
