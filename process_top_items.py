import multiprocessing
import setproctitle
from put_data import PutData
from config import Config
from utils import MeasureTime
from time import time
from functools import lru_cache

class ProcessTopItems(multiprocessing.Process, Config):

    timers = {}

    def __init__(self, name: str, prod2cat: dict, categories: dict, children_full: dict, products_to_calc: set, input_q: multiprocessing.SimpleQueue):
        multiprocessing.Process.__init__(self)
        self.name = name
        self.prod2cat = prod2cat
        self.categories = categories
        self.children_full = children_full
        self.products_to_calc = products_to_calc
        self.input_q = input_q

    def run(self):
        setproctitle.setproctitle(self.name)
        top_items = {}
        count = 1
        s = time()

        while True:
            with MeasureTime(timers=self.timers, timer_name='get'):
                chunk = self.input_q.get()

            if chunk == 'STOP':
                break
            print(' '.join([' '*40, self.name, 'GET', str(count)]))
            count += 1
            len_chunk = len(chunk)
            if len_chunk == 0:
                continue
            with MeasureTime(timers=self.timers, timer_name='top_items'):
                for p1, p2, coef in chunk:
                    cat1 = self.prod2cat[p1]
                    cat2 = self.prod2cat[p2]
                    if cat1 > cat2:
                        # ensure always cat1 <= cat2, this is in order for lru_cache on get_relation to works better
                        cat2, cat1 = cat1, cat2
                    relation = self.get_relation(cat1, cat2)

                    if p1 in self.products_to_calc:  # only calculate the products we want
                        p1_relation = (p1, relation)
                        if p1_relation not in top_items:
                            top_items[p1_relation] = []
                        top_items[p1_relation] = self.get_top(top_items[p1_relation], [p2, coef], self.MAX_ITEMS_TO_KEEP)

                    if p2 in self.products_to_calc:
                        p2_relation = (p2, relation)
                        if p2_relation not in top_items:
                            top_items[p2_relation] = []
                        top_items[p2_relation] = self.get_top(top_items[p2_relation], [p1, coef], self.MAX_ITEMS_TO_KEEP)
        print('process_top_items end', len(top_items), 'products')
        self.timers['all'] = time() - s
        print('process', self.name, self.timers)
        put_data = PutData()
        put_data.put_data(top_items)
        print('process_top_items DONE')

    @staticmethod
    def get_top(current, new, max_count):
        if len(current) == max_count and current[9][1] > new[1]:
            return current

        current.append(new)
        return sorted(current, key=lambda x: x[1], reverse=True)[0:max_count]

    @lru_cache(maxsize=None)
    def get_relation(self, cat1, cat2):
        if cat1 == cat2:  # if in the same category 0
            return 0
        i = 1
        parent = self.categories[cat1]
        while True:
            # increase i for each parent
            if parent == 0 or cat2 in self.children_full[parent]:
                break
            i += 1
            parent = self.categories[parent]
        return i
