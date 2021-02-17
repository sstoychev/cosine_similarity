"""
https://realpython.com/python-timer/

Decorator and context manager for time measurements. The results are written in
the passed timers parameter. We are using that mutable objects are passed by
reference and their value can be changed from the called function


You can have multiple timers each in the timers dictionary
We need a dictionary or a list because they are mutable and the changes
we do here are affecting the variable in the calling class.
More about passing values by reference:
https://stackoverflow.com/questions/986006/how-do-i-pass-a-variable-by-reference

Example:

def my_func1:
    a = 1 + 1

def my_func2:
    a = 2 + 2

@measure_time
def my_func3:
    a = 3 + 3

timers = {}

with MeasureTime(timers=timers, 't1')
    my_func1()
with MeasureTime(timers=timers, 't2')
    my_func2()

my_func3()

print(timers)

"""
from functools import wraps
from time import time


def measure_time(timers: dict, timer_name: str = ''):
    """
    Decorator

    :param timers:
    :param timer_name:
    :return:
    """
    def actual_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tic = time()
            value = func(*args, **kwargs)
            if timer_name not in timers:
                timers[timer_name] = 0

            timers[timer_name] += time() - tic

            return value
        return wrapper
    return actual_decorator


class MeasureTime:
    """
    context manager class
    """
    start = 0

    def __init__(self, timers: dict, timer_name: str = ''):
        self.timers = timers
        self.timer_name = timer_name

    def __enter__(self):
        self.start = time()

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.timer_name not in self.timers:
            self.timers[self.timer_name] = 0
        self.timers[self.timer_name] += time() - self.start
