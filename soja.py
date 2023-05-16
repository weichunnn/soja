import argparse
import csv
import multiprocessing as mp
import sys
import time
import tracemalloc


def hash(element):
    """Returns the value of the first element of a given input.

    Args:
        element (int): A tuple of data where the first element is the join attribute and is an integer.

    Returns:
        hash_value (int): The hash value of the first element.
    """
    return element[0]
    # total = 0
    # for digit in str(element[0]):
    #     total += int(digit)
    # return total


def create_hash_table(table):
    """Creates a hash table based on the elements of the input table.

    Args:
        table (list): A list of tuple, each containing the data

    Returns:
        hash_table (dict): The hash table with each tuple hashed based on the first element
    """
    hash_table = {}
    for element in table:
        if hash(element) in hash_table:
            hash_table[hash(element)].append(element)
        else:
            hash_table[hash(element)] = [element]
    return hash_table


def lookup(element, hash_table):
    """Look up an element in a hash table and check if it exist.

    Args:
        element: The element to look up in the hash table.
        hash_table: The hash table to search in.

    Returns:
        A tuple containing a boolean value indicating if any matches were found, and a list of the matches.
    """
    hash_value = hash(element)
    matches = []
    if hash_value in hash_table:
        for item in hash_table[hash_value]:
            # there could be multiple matches hence we don't break (e.g in a 1-many relationship)
            if element[0] == item[0]:
                matches.append(element + item[1:])
    return len(matches) > 0, matches


def process(R, hash_table, dangling_tuples):
    """Perform the join operation between table R and table S (in the format of a hash table). The function
    also keep tracks of marking the dangling tuples after each iteration.

    Args:
        R (list): The list of elements (table R) to process.
        hash_table (dict): The hash table (table S) to perform lookups in.
        dangling_tuples (list): A list of indices representing dangling tuples.

    Returns:
        tuple: A tuple containing the result list and the updated dangling_tuples list.
    """
    result = []
    for i, element in enumerate(R):
        is_exist, matches = lookup(element, hash_table)
        if is_exist:
            result += matches
            # remove from dangling tuples if not previously seen before
            dangling_tuples.remove(i) if i in dangling_tuples else None
    return result, dangling_tuples


def roundrobin_partition(data, number_of_processor):
    """Partition a list of data into multiple sublists using a round-robin algorithm.

    Args:
        data (list): The list of data to partition.
        number_of_processor (int): The number of processors to partition the data into.

    Returns:
        list: A list containing the global partitions, where each partition is a sublist.
    """
    # create partitions equal number of processors
    global_partitions = [[] for i in range(number_of_processor)]
    for index, element in enumerate(data):
        index_bin = (int)(index % number_of_processor)
        # add data to the partition based on the index
        global_partitions[index_bin].append(element)
    return global_partitions


def worker(input_queue, next_queue, max_iteration, output_file_path):
    """Perform iterative processing of data (outer joins) in a worker.

    Args:
        input_queue (Queue): The input queue from which data is retrieved.
        next_queue (Queue): The next queue to which processed data is passed for further processing.
        max_iteration (int): The maximum number of iterations to perform.
        output_file_path (str): The file path of the output file where temporary and dangling results are written.

    Returns:
        None
    """
    iteration = 0
    S_table = None
    S_len = 0

    # create csv writer to write temporary data
    output_file = open(output_file_path, "a")
    csv_writer = csv.writer(output_file)

    while True:
        # get() is blocking until there is data in the queue
        R, S, dangling_tuples = input_queue.get()
        if not S_table:
            # create hash table only in the first iteration
            S_table = create_hash_table(S)
            S_len = len(S[0])

        # stop when worker has finish processing all data
        # and write the remaining dangling tuples to file
        if iteration == max_iteration:
            dangling = []
            for i in dangling_tuples:
                res = R[i] + tuple([None] * (S_len - 1))
                csv_writer.writerow(res)
            break

        # process the current R and S
        result, updated_dangling_tuples = process(R, S_table, dangling_tuples)
        # write inner join result to file for current iteration
        csv_writer.writerows(result)
        # transfer R to the next worker with updated dangling tuples
        next_queue.put((R, None, updated_dangling_tuples))
        iteration += 1

    output_file.close()  # close filet to prevent memory leak


def soja(R, S, number_of_processor, output_file_path):
    """Perform a distributed outer join using the SOJA algorithm.

    Args:
        R (list): The list of elements (table R) to process.
        S (list): The list of elements (table S) to process.
        number_of_processor (int): The number of processors to partition the data into and for parallel processing.
        output_file_path (str): The file path of the output file where temporary and dangling results are written.

    Returns:
        tuple: A tuple containing the elapsed time (in seconds) and the total memory used (in bytes).
    """
    # prerequisite that R and S are partioned equally
    # this is used for DEMO purpose only as in the ideal world, data had already been partitoned equally
    R_partitions = roundrobin_partition(R, number_of_processor)
    S_partitions = roundrobin_partition(S, number_of_processor)

    # start timer and memory profiler
    tracemalloc.start()
    start_time = time.perf_counter()

    # create queues for communication between workers
    process_queues = [mp.Queue(maxsize=1) for i in range(number_of_processor)]
    process_list = []

    # clear file in case it already exists
    open(output_file_path, "w").close()

    for i in range(number_of_processor):
        next_node = (i + 1) % number_of_processor
        # initialize worker process with input and next queues
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

    # start the worker processes in blocking manner
    for process in process_list:
        process.start()

    # initialize each worker's queue with corresponding R and S partitions and the dangling tuple set
    for i, q in enumerate(process_queues):
        q.put(
            (
                R_partitions[i],
                S_partitions[i],
                set([i for i in range(len(R_partitions[i]))]),
            )
        )

    # wait for all processes to finish
    for p in process_list:
        p.join()

    # stop timer and memory profiler
    # calculate elapsed time and total memory used
    elpased_time = time.perf_counter() - start_time

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")

    total_memory = sum(stat.size for stat in top_stats)

    return elpased_time, total_memory


def read_csv(filename):
    """Read data from a CSV file.

    Args:
        filename (str): The path to the CSV file.

    Returns:
        list: A list of tuples representing the data from the CSV file.
    """
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
