import contextlib
import functools
import random
import time
import celery
import memcache

app = celery.Celery(__name__, broker='amqp://guest@localhost//')
TOTAL_PROCESSING_TIME = 8
LOCK_EXPIRE = 60 * 5 # Lock expires in 5 minutes


class OnlyOnce(object):

    def __init__(self, func):
        self.wrapped_func = func
        self.cache = memcache.Client(['127.0.0.1', '11211'])

        # this achieves the same as the @wraps decorator,
        # except for the present class decorator instead.
        functools.update_wrapper(self, func)

    def __call__(self, *args):
        with self.lock(args) as acquired:
            if acquired:
                self.wrapped_func(*args)
            else:
                print "This task is already being handled"

    @contextlib.contextmanager
    def lock(self, args):
        lock_id = self.generate_lock_id(args)
        acquire_lock = lambda: self.cache.add(lock_id, 'true', time=LOCK_EXPIRE)
        release_lock = lambda: self.cache.delete(lock_id)

        try:
            yield acquire_lock()
        finally:
            release_lock()

    def generate_lock_id(self, args):
        return "%s-lock-%s" % (
            self.wrapped_func.func_name,
            ','.join(str(a) for a in args)
        )


def elapsed_time(start):
    return time.time() - start


@app.task
@OnlyOnce
def add(a, b):
    print "{a} + {b} = ???".format(a=a, b=b)
    start = time.time()
    while elapsed_time(start) < TOTAL_PROCESSING_TIME:
        print "processing... ({}% complete)".format(
            round(100 * (elapsed_time(start) / TOTAL_PROCESSING_TIME), 1)
        )
        time.sleep(random.randint(5,15) / 10.0)

    print "{a} + {b} = {result}".format(a=a, b=b, result=a+b)
    return a + b
