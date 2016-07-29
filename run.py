#!/usr/bin/env python

import random
import time
import tasks

def main():

    a, b = random.randint(0, 10), \
           random.randint(0, 10)
    tasks.add.delay(a, b)
    time.sleep(0.15)
    tasks.add.delay(a, b)
    tasks.add.delay(a, b)
    tasks.add.delay(a, b)


if __name__ == '__main__':
    main()
