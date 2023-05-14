import argparse
import csv
import multiprocessing as mp
import sys
import time
import tracemalloc


def hash(element):
    return element[0]
    # total = 0
    # for digit in str(element[0]):
    #     total += int(digit)
    # return total


def create_hash_table(table):
    hash_table = {}
    for element in table:
        if hash(element) in hash_table:
            hash_table[hash(element)].append(element)
        else:
            hash_table[hash(element)] = [element]
    return hash_table


def lookup(element, hash_table):
    hash_value = hash(element)
    matches = []
    if hash_value in hash_table:
        for item in hash_table[hash_value]:
            if element[0] == item[0]:
                matches.append(element + item[1:])
    return len(matches) > 0, matches


def process(R, hash_table, dangling_tuples):
    result = []
    for i, element in enumerate(R):
        is_exist, matches = lookup(element, hash_table)
        if is_exist:
            result += matches
            # remove from dangling tuples if not previously seen before
            dangling_tuples.remove(i) if i in dangling_tuples else None
    return result, dangling_tuples


def roundrobin_partition(data, number_of_processor):
    global_partitions = [[] for i in range(number_of_processor)]
    for index, element in enumerate(data):
        index_bin = (int)(index % number_of_processor)
        global_partitions[index_bin].append(element)
    return global_partitions


def worker(input_queue, next_queue, max_iteration, output_file_path):
    iteration = 0
    S_table = None
    S_len = 0

    output_file = open(output_file_path, "a")
    csv_writer = csv.writer(output_file)

    while True:
        R, S, dangling_tuples = input_queue.get()  # blocking until there is data
        if not S_table:
            S_table = create_hash_table(S)
            S_len = len(S[0])
        if iteration == max_iteration:
            dangling = []
            for i in dangling_tuples:
                res = R[i] + tuple([None] * (S_len - 1))
                csv_writer.writerow(res)
            break

        result, updated_dangling_tuples = process(R, S_table, dangling_tuples)
        now = time.time()
        csv_writer.writerows(result)
        next_queue.put((R, None, updated_dangling_tuples))
        iteration += 1

    output_file.close()


def soja(R, S, number_of_processor, output_file_path):
    # prerequisite that R and S are partioned equally
    R_partitions = roundrobin_partition(R, number_of_processor)
    S_partitions = roundrobin_partition(S, number_of_processor)

    tracemalloc.start()
    start_time = time.perf_counter()

    process_queues = [mp.Queue(maxsize=1) for i in range(number_of_processor)]
    process_list = []

    open(output_file_path, "w").close()  # clear file

    for i in range(number_of_processor):
        next_node = (i + 1) % number_of_processor
        p = mp.Process(
            target=worker,
            args=(
                process_queues[i],
                process_queues[next_node],
                number_of_processor,
                output_file_path,
            ),
        )
        process_list.append(p)

    for process in process_list:
        process.start()

    for i, q in enumerate(process_queues):
        q.put(
            (
                R_partitions[i],
                S_partitions[i],
                set([i for i in range(len(R_partitions[i]))]),
            )
        )

    for p in process_list:
        p.join()

    elpased_time = time.perf_counter() - start_time

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")

    total_memory = sum(stat.size for stat in top_stats)

    return elpased_time, total_memory


def read_csv(filename):
    with open(filename, "r") as f:
        data = f.readlines()[1:]  # skip first header line
    return [tuple(line.strip().split(",")) for line in data]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--R-file", help="Path to file acting as left table", required=True
    )
    parser.add_argument(
        "--S-file", help="Path to file acting as right table", required=True
    )
    parser.add_argument(
        "--concurrency-count",
        help="Number of parallel concurrent run",
        required=False,
        type=int,
        default=mp.cpu_count(),
    )
    parser.add_argument(
        "--output-file",
        help="Output file path",
        required=False,
        default="output-soja.csv",
    )
    args = parser.parse_args()
    R, S = read_csv(args.R_file), read_csv(args.S_file)

    elpased_time, memory_usage = soja(R, S, args.concurrency_count, args.output_file)
    print(f"Memory usage: {memory_usage / 1024 / 1024:.2f} MB")
    print(f"Execution time: {elpased_time:.2f} seconds")
    print("-------------------------")
