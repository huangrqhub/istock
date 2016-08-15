#!/usr/bin/env python
#coding=utf-8

from multiprocessing import Pool
from time import sleep
from ga1 import *

def main():
    pool = Pool(processes=20)    # set the processes max number 3
    sc = sc_get()
    for i in sc:
        result = pool.apply_async(get_result, i)
    pool.close()
    pool.join()
    if result.successful():
        print 'successful'


if __name__ == "__main__":
    main()

