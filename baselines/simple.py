# This is a simple MapReduce example with the simple shuffle mechanism through
# Ray Core. The definition of simple shuffle is define in Figure 2 of the
# Exoshuffle paper.
#
# Reference:
# https://docs.ray.io/en/latest/ray-core/examples/map_reduce.html (Ray Core, MapReduce)
# https://dl.acm.org/doi/pdf/10.1145/3603269.3604848 (Exoshuffle)

import subprocess
import ray


def map_function(document):
    for word in document.lower().split():
        yield word, 1


@ray.remote
def apply_map(corpus, num_partitions=3):
    map_results = [list() for _ in range(num_partitions)]
    for document in corpus:
        for result in map_function(document):
            first_letter = result[0].decode("utf-8")[0]
            word_index = ord(first_letter) % num_partitions
            map_results[word_index].append(result)
    return map_results


@ray.remote
def apply_reduce(*results):
    reduce_results = dict()
    for res in results:
        for key, value in res:
            if key not in reduce_results:
                reduce_results[key] = 0
            reduce_results[key] += value

    return reduce_results


def simple_shuffle(corpus: list, num_m: int, num_r: int, debug: bool = False):
    """
    This function implements the simple shuffle through Ray.

    Parameters:
    -----------
    corpus: list
        the dataset to be counted and sorted.
    num_m: int
        the number of Mappers.
    num_r: int
        the number of Reducers.
    debug: bool
        if Ture, print the shuffle and reduce results.
    """

    chunk = len(corpus) // num_m
    remainder = len(corpus) % num_m
    partitions = [corpus[i * chunk : (i + 1) * chunk] for i in range(num_m)]
    if remainder:
        partitions[-1].extend(corpus[num_m * chunk :])

    # =====================================
    # Map Stage.
    # =====================================

    map_results = [
        apply_map.options(num_returns=num_r).remote(data, num_r) for data in partitions
    ]

    # =====================================
    # Shuffle and Reduce Stage.
    # =====================================
    if debug:
        print("\nShuffle Results:")
        for i in range(num_m):
            mapper_results = ray.get(map_results[i])
            for j, result in enumerate(mapper_results):
                print(f"Partial results from Mapper {i} to Reducer {j}: {result[:2]}")

    # results in each Reducer.
    outputs = []

    for i in range(num_r):
        outputs.append(
            apply_reduce.remote(*[partition[i] for partition in map_results])
        )

    counts = {k: v for output in ray.get(outputs) for k, v in output.items()}

    sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)

    if debug:
        print("\nSorted Counts:")
        for count in sorted_counts:
            print(f"{count[0].decode('utf-8')}: {count[1]}")


if __name__ == "__main__":

    # data loading.
    zen_of_python = subprocess.check_output(["python", "-c", "import this"])
    corpus = zen_of_python.split()

    simple_shuffle(corpus, 5, 4, False)
    simple_shuffle(corpus, 3, 3, True)
