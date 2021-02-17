import multiprocessing
import setproctitle
from time import time
from math import sqrt
from operator import mul
from functools import partial
from utils import MeasureTime
from config import Config


class ProcessCouples(multiprocessing.Process, Config):

    timers = {}

    def __init__(self, name, v_groups, vectors, input_q, output_q):
        """
        :param v_groups: all vector groups in format [[1,2,3,4], [5,6,7,8], [9,10,11,12]] where the numbers are indexes in vectors
        :param input_q: the elements are in format [product_id, group_id]
        :param output_q: we put the generated couples here
        :return:

        This function will be in the processes to generate couples of products.
        From input_q we get for which Queue we should generate the couples. For example if we get [1,6] from input queue
        we should generate combinations from [5,6,7,8] group for elements 6,7,8 => 67,68,78
        """
        multiprocessing.Process.__init__(self)
        self.name = name
        self.v_groups = v_groups
        self.vectors = vectors
        self.input_q = input_q
        self.output_q = output_q

    def get(self):
        return self.input_q.get()

    def put(self, value):
        self.output_q.put(value)

    def run(self):

        s = time()
        setproctitle.setproctitle(self.name)
        current_chunk = []
        count = 0
        for product_id, group_id in iter(partial(self.input_q.get), 'STOP'):
            v_group = self.v_groups[group_id]
            p1 = v_group[product_id]
            print('{:<15}'.format(self.name), ' GET ', product_id, p1)
            for i in range(product_id+1, len(v_group)):
                p2 = v_group[i]
                with MeasureTime(timers=self.timers, timer_name='cos'):
                    coef = self.cosine_similarity(self.vectors[p1], self.vectors[p2])
                coef = int(coef * 100)
                if coef == 0:
                    continue
                with MeasureTime(timers=self.timers, timer_name='append'):
                    current_chunk.append((p1, p2, coef))
                count += 1
                if (count % self.CHUNK_SIZE) == 0:
                    # self.output_q.put(current_chunk)
                    with MeasureTime(timers=self.timers, timer_name='put'):
                        self.output_q.put(current_chunk)
                    current_chunk.clear()  # empty the list, just in case
        if current_chunk:
            self.output_q.put(current_chunk)
            current_chunk.clear()  # empty the list
        self.timers['all'] = time() - s
        print('process', self.name, 'end', 'generated couples', count,)
        print('process', self.name, self.timers)

    @staticmethod
    # @measure_time(timers, 'cos')
    def cosine_similarity(v1, v2):
        """
        compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)
        instead of sparse vectors full with zeros, the vectors could be filled only with the valid positions
        """
        # we want v1 to be the longer vector
        if len(v2) > len(v1):
            v1, v2 = v2, v1

        # sumxx, sumxy, sumyy = 0, 0, 0
        # # calc the longer vector and x*y
        # for v, x in v1.items():
        #     sumxx += x * x
        #     if v in v2:
        #         # y = v2[v]  # in future this may not be 1
        #         sumxy += x * v2[v]
        # for v, y in v2.items():
        #     sumyy += y * y

        sumxx = sum(map(mul, v1.values(), v1.values()))
        sumyy = sum(map(mul, v2.values(), v2.values()))
        sumxy = sum((v1[k] * v2[k] for k in v2 if k in v1))
        # for k in v2:
        #     if k in v1:
        #         sumxy += v1[k] * v2[k]
        return sumxy / sqrt(sumxx * sumyy)
