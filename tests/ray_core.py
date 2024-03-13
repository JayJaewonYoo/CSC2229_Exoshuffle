# This is a test file for Ray Core, including tasks, actors, and objects.
# Reference: https://docs.ray.io/en/latest/ray-core/walkthrough.html

import ray
import numpy as np
import time

# =====================================
# Running a Task
# =====================================


@ray.remote
def square(x: int):
    """
    This function is used to run a square function as a remote task in the
    cluster via Ray.
    """

    return x**2


def launch_tasks():
    """
    This function is used to launch parallel square tasks. Each remote
    function will return a future, which is a Ray object reference, that can be
    then fetched with ray.get().
    """

    futures = [square.remote(i) for i in range(5)]
    assert ray.get(futures) == [0, 1, 4, 9, 16]


# =====================================
# Calling an Actor
# =====================================


@ray.remote
class Counter:
    """
    Defines the Counter class as a Ray actor.
    """

    def __init__(self):
        self.i = 0

    def get(self):
        return self.i

    def incr(self, value):
        self.i += value


def launch_actor():
    """
    This function is used to use the Ray actor to execute remote method calls
    and maintain internal states of a class.
    """

    # creates a Counter class.
    c = Counter.remote()

    # submits calls to the actor, and these calls will run asynchronously but in
    # submission order on the remote actor process.
    for _ in range(10):
        c.incr.remote(1)

    # retrieve final results.
    assert ray.get(c.get.remote()) == 10


# =====================================
# Passing an Object
# =====================================


# Define a task that sums the values in a matrix.
@ray.remote
def sum_matrix(matrix):
    return np.sum(matrix)


def pass_obj():
    """
    There are two options to fetch the results via Ray. Firstly, Ray stores task
    and actor call results in its distributed object store, returning object
    references that can be later retrieved. Secondly, object references can also
    be created explicitly via ray.put(), and they can be passed to tasks as
    substitutes for argument values.
    """

    # calls the task with a literal argument value.
    sum1 = ray.get(sum_matrix.remote(np.ones((100, 100))))

    # puts a larger array into the object store.
    matrix_ref = ray.put(np.ones((1000, 1000)))

    # call the task with the object reference as an argument.
    sum2 = ray.get(sum_matrix.remote(matrix_ref))

    assert sum1 == 10000.0
    assert sum2 == 1000000.0


# =====================================
# Specifying Required Resources and Passing Obj Refs
# =====================================


@ray.remote(num_cpus=1)
def my_func(x: int = 1):
    return x


def specify_resources():
    """
    This function shows how to specify computing resources via Ray.
    """

    # invokes the function with default 2 cpus.
    obj_ref_1 = my_func.remote()

    # overrides the default resource requirements.
    obj_ref_2 = my_func.options(num_cpus=2).remote(2)

    # passes an object ref as an argument to another Ray task, and note that
    # this will be blocked until the task of obj_ref_2 finished.
    obj_ref_3 = my_func.remote(obj_ref_2)

    assert ray.get(obj_ref_1) == 1
    assert ray.get(obj_ref_2) == 2
    assert ray.get(obj_ref_3) == 2


# =====================================
# Waiting for Partial Results
# =====================================


@ray.remote
def slow_function(i):
    time.sleep(1)
    return i


def wait():
    object_refs = [slow_function.remote(i) for i in range(2)]
    # returns as soon as one of the tasks finished execution.
    ready_refs, remaining_refs = ray.wait(object_refs, num_returns=1, timeout=None)
    assert (ready_refs, remaining_refs) == (1, 2) or (2, 1)


if __name__ == "__main__":
    """
    Note that we do not need to call init() and shutdown() manually!

    ray.init() will be automatically called on the first use of a Ray remote
    API, while ray.shutdown() will automatically run at the end when a Python
    process that uses Ray exits.
    """

    ray.init()

    launch_tasks()
    launch_actor()
    pass_obj()
    specify_resources()
    wait()

    # If Ray works normally, the main function will print Ture.
    print(True)
    ray.shutdown()
