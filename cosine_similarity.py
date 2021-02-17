# https://badubg1.atlassian.net/wiki/spaces/BR/pages/15040513/Recommendations+suggestions+of+products+to+the+users
# https://www.tutorialspoint.com/python/python_multithreading.htm

import datetime
import time
import multiprocessing as mp

from process_couples import ProcessCouples
from process_top_items import ProcessTopItems
from config import Config
from get_data import GetData


###################
# MAIN
###################
def main():
    mp.set_start_method('spawn')
    config = Config()
    s = datetime.datetime.now()
    print(s)

    get_data = GetData()
    vectors, vector_groups, prod2cat, categories, children_full = get_data.get_data()

    # calculate similarity
    print('get product couples', (datetime.datetime.now() - s).total_seconds(), 'secs')

    generate_chunks_q = mp.Queue()
    top_items_q = mp.SimpleQueue()

    gen_chunks_processes = []

    # Fill the work queue
    print('Fill generate_chunks_q')
    count = 0
    products_to_calc = set()
    for i, vg in enumerate(vector_groups):
        vg_len = len(vg)
        for k in range(0, vg_len - 1):
            generate_chunks_q.put([k, i])
            products_to_calc.add(vg[k])
            count += 1
            if count >= config.GENERATE_CHUNKS_PROCESS_COUNT * 16*4:
                break

    print('start {} generate chuncks processes'.format(config.GENERATE_CHUNKS_PROCESS_COUNT))
    for i in range(config.GENERATE_CHUNKS_PROCESS_COUNT):
        p = ProcessCouples('Chunks-' + str(i), vector_groups, vectors, generate_chunks_q, top_items_q)
        p.start()
        gen_chunks_processes.append(p)
        generate_chunks_q.put('STOP')  # put value for each process to stop

    top_20_process = ProcessTopItems('Top20 process', prod2cat, categories, children_full, products_to_calc, top_items_q)
    top_20_process.start()

    print('calc top {}:'.format(config.MAX_ITEMS_TO_KEEP), (datetime.datetime.now() - s).total_seconds(), 'secs')

    print('waiting generate_chunks_q to be processed', generate_chunks_q.empty())
    while not generate_chunks_q.empty():
        print('generate_chunks_q not empty', (datetime.datetime.now() - s).total_seconds(), 'secs')
        time.sleep(5)

    # stop similarity calculation processes
    for i, p in enumerate(gen_chunks_processes):
        print(f'waiting gen_chunks_processes {i} to finish')
        p.join()

    # ensure that the queue is empty
    print('send message to top_items to stop')
    top_items_q.put('STOP')

    print('waiting the top_items queue to be processed')
    while not top_items_q.empty():
        print('top_items not empty')
        time.sleep(5)

    print('join process top_20_process')
    while top_20_process.is_alive():
        print('top_20_process is alive', (datetime.datetime.now() - s).total_seconds(), 'secs')
        time.sleep(5)

    print('generate_chunks_q empty', generate_chunks_q.empty(),
          'top_items_q empty', top_items_q.empty(),
          (datetime.datetime.now() - s).total_seconds(), 'secs')


if __name__ == '__main__':
    main()
