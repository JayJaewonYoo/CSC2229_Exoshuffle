# This is a sample python script for running CloudSort through exoshuffle on a
# single node.

import ray
import logging


class JobConfig:
    def __init__(self):
        self.name = "cloudsort"
        self.num_mappers = 4
        self.num_reducers = 4
        self.f = 2
        assert self.num_mappers % self.f == 0
        self.num_merger = int(self.num_mappers / self.f)

        self.num_input = 20
        self.base_path = "./data/generated_data/partitioned_data_"


def log_init():
    fmt = "[%(levelname)s %(asctime)s %(filename)s:%(lineno)s] %(message)s"
    logging.basicConfig(
        format=fmt,
        level=logging.INFO,
    )
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)


def ray_init():
    ray.init()
    resources = ray.cluster_resources()
    logging.info("Cluster resources: %s", resources)
    # TODO: connects workers with ips in distributed environment.


def init(cfg: JobConfig):
    log_init()
    logging.info("Job config: %s", cfg.name)
    ray_init()


def load_data(num_input: int, base_path: str):
    data = []
    for i in range(num_input):
        file_path = base_path + str(i)
        with open(file_path, "rb") as f:
            lines = f.readlines()
            data.append(lines)
            logging.info(f"Number of records in {file_path} is {len(lines)}")
    return data


@ray.remote
def apply_map(corpus, num_partitions: int):
    map_results = [list() for _ in range(num_partitions)]
    for record in corpus:
        # first 8 bytes as key
        key = int.from_bytes(record[:8], "big")
        record_index = key % num_partitions
        map_results[record_index].append((key, record))
    map_results = [sorted(result, key=lambda x: x[0]) for result in map_results]
    return map_results


@ray.remote
def apply_merge(num_r, *map_results):
    resolved_map_results = [ray.get(result) for result in map_results]
    merge_results = []
    for i in range(num_r):
        merged_group = [item for sublist in resolved_map_results for item in sublist[i]]
        merge_results.append(merged_group)
    merge_results = [sorted(result, key=lambda x: x[0]) for result in merge_results]
    return merge_results


@ray.remote
def apply_reduce(*results):
    reduce_results = dict()
    for res in results:
        for key, value in res:
            if key not in reduce_results:
                reduce_results[key] = []
            reduce_results[key].append(value)
    for key in reduce_results:
        reduce_results[key].sort(key=lambda x: x[8:10])

    sorted_list = sorted(
        [(k, v) for k, v in reduce_results.items()], key=lambda x: x[0]
    )
    flattened_array = [item for sublist in sorted_list for item in sublist[1]]
    return flattened_array


def main():
    job_cfg = JobConfig()
    init(job_cfg)

    # data loading
    logging.info("Loading data")
    data = load_data(job_cfg.num_input, job_cfg.base_path)

    # map stage
    logging.info("Map stage started.")
    map_results = [
        apply_map.options(num_returns=job_cfg.num_reducers).remote(
            partitioned_data, job_cfg.num_reducers
        )
        for partitioned_data in data
    ]
    logging.info(
        f"Map stage completed, with {len(map_results)} results, each has {job_cfg.num_reducers} partitions."
    )

    # merge stage
    logging.info("Merge stage started.")
    merge_results = [
        apply_merge.options(num_returns=job_cfg.num_reducers).remote(
            job_cfg.num_reducers, *map_results
        )
        for i in range(job_cfg.num_merger)
    ]
    logging.info(
        f"Merge stage completed, with {len(merge_results)} results, each has {job_cfg.num_reducers} partitions."
    )

    # reduce stage
    logging.info("Reduce stage started.")
    outputs = []
    for i in range(job_cfg.num_reducers):
        outputs.append(
            apply_reduce.remote(*[partition[i] for partition in merge_results])
        )
    # print(len(outputs))
    # print(ray.get(outputs[0]))
    logging.info("Reduce stage completed.")


if __name__ == "__main__":
    main()
