# This is a riffle shuffle implementation through Ray Core. The riffle shuffle
# (pre-shuffle merge) is define in Figure 2 of the Exoshuffle paper.
#
# Reference:
# https://dl.acm.org/doi/pdf/10.1145/3603269.3604848 (Exoshuffle)
# https://dl.acm.org/doi/pdf/10.1145/3190508.3190534 (Riffle)

import subprocess
import ray


def map_function(document):
    for word in document.lower().split():
        yield word, 1


@ray.remote
def apply_map(corpus, num_partitions: int = 3):
    map_results = [list() for _ in range(num_partitions)]
    for document in corpus:
        for result in map_function(document):
            first_letter = result[0].decode("utf-8")[0]
            word_index = ord(first_letter) % num_partitions
            map_results[word_index].append(result)
    return map_results


# TODO: bug here!
@ray.remote
def apply_merge(num_r, *map_results):
    # resolved_map_results = ray.get(list(map_results))
    merge_results = []
    for i in range(num_r):
        merged_group = [sum(mapper_results, []) for mapper_results in zip(*map_results)]
        # merged_group = [
        #     item
        #     for mapper_results in zip(*resolved_map_results)
        #     for item in mapper_results
        # ]
        merge_results.append(merged_group)
    return merge_results


@ray.remote
def apply_reduce(*results):
    reduce_results = dict()
    for res in results:
        for key, value in res:
            if key not in reduce_results:
                reduce_results[key] = 0
            reduce_results[key] += value

    return reduce_results


def riffle_shuffle(corpus: list, num_m: int, num_r: int, f: int, debug: bool = False):
    """
    This function implements the riffle shuffle through Ray.

    Parameters:
    -----------
    corpus: list
        the dataset to be counted and sorted.
    num_m: int
        the number of Mappers.
    num_r: int
        the number of Reducers.
    f: int
        the merging factor.
    debug: bool
        if Ture, print the shuffle and reduce results.
    """

    if num_m % f != 0:
        raise ValueError("num_m must be a multiple of f!")

    # =====================================
    # Map Stage.
    # =====================================

    chunk = len(corpus) // num_m
    remainder = len(corpus) % num_m
    partitions = [corpus[i * chunk : (i + 1) * chunk] for i in range(num_m)]
    if remainder:
        partitions[-1].extend(corpus[num_m * chunk :])

    map_results = [
        apply_map.options(num_returns=num_r).remote(data, num_r) for data in partitions
    ]

    # =====================================
    # Merge Stage.
    # =====================================

    num_merge = int(num_m / f)
    merge_results = [
        apply_merge.options(num_returns=num_r).remote(num_r, *map_results[i : i + f])
        for i in range(num_merge)
    ]

    # =====================================
    #  Reduce Stage.
    # =====================================
    if debug:
        print("\nShuffle Results (after Merge Stages):")
        for i in range(num_merge):
            merger_results = ray.get(merge_results[i])
            for j, result in enumerate(merger_results):
                print(f"Partial results from Merger {i} to Reducer {j}: {result[:2]}")

    # results in each Reducer.
    outputs = []

    for i in range(num_r):
        outputs.append(
            apply_reduce.remote(*[partition[i] for partition in merge_results])
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

    riffle_shuffle(corpus, 6, 4, 3, False)
