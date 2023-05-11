import multiprocessing as mp
import sys

def hash(element):
    total = 0
    for digit in str(element[0]):
        total += int(digit)
    return total


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


def worker(input_queue, next_queue, max_iteration, return_result):
    iteration = 0
    S_table = None
    S_len = 0
    while True:
        R, S, dangling_tuples = input_queue.get()  # blocking until there is data
        if iteration == max_iteration:
            dangling = []
            for i in dangling_tuples:
                res = R[i] + tuple([None] * (S_len - 1))
                return_result.append(res)
            break
        if not S_table:
            S_table = create_hash_table(S)
            S_len = len(S[0])

        result, updated_dangling_tuples = process(R, S_table, dangling_tuples)
        return_result += result

        next_queue.put((R, None, updated_dangling_tuples))
        iteration += 1


def soja(R, S, number_of_processor):
    # prerequisite that R and S are partioned equally
    R_partitions = roundrobin_partition(R, number_of_processor)
    S_partitions = roundrobin_partition(S, number_of_processor)

    process_queues = [mp.Queue() for i in range(number_of_processor)]
    process_list = []

    manager = mp.Manager()
    return_result = manager.list()

    for i in range(number_of_processor):
        next_node = (i + 1) % number_of_processor
        p = mp.Process(
            target=worker,
            args=(
                process_queues[i],
                process_queues[next_node],
                number_of_processor,
                return_result,
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

    return return_result


def get_dataset():
    R = [
        (8, "Adele"),
        (22, "Bob"),
        (16, "Clement"),
        (23, "Dave"),
        (11, "Ed"),
        (25, "Fung"),
        (3, "Goel"),
        (17, "Harry"),
        (14, "Irene"),
        (2, "Joanna"),
        (6, "Kelly"),
        (20, "Lim"),
        (1, "Meng"),
        (5, "Noor"),
        (19, "Omar"),
    ]
    S = [
        (8, "Arts"),
        (15, "Business"),
        (2, "CompSc"),
        (12, "Dance"),
        (7, "Engineering"),
        (21, "Finance"),
        (10, "Geology"),
        (11, "Health"),
        (18, "IT"),
    ]
    return R, S


def read_csv(filename):
    with open(filename, "r") as f:
        data = f.readlines()[1:]  # skip first header line
    return [tuple(line.strip().split(",")) for line in data]


if __name__ == "__main__":
    old_stdout = sys.stdout
    log_file = open("message.log","w")
    sys.stdout = log_file

    R, S = read_csv("test/movies.csv"), read_csv("test/ratings.csv")
    join_res = soja(R, S, mp.cpu_count())
    print(join_res)

    sys.stdout = old_stdout
    log_file.close()
