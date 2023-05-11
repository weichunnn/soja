import argparse
import csv
import multiprocessing as mp
import time
import tracemalloc


def hash_distribution(T, n):
    """distribute data using hash partitioning

    T -- a list of records
    n -- number of partitions

    """

    # Define a simple hash function for demonstration
    def s_hash(x, n):
        h = x % n
        return h

    result = {}
    for t in T:
        # hashes the join attribute
        h_key = s_hash(int(t[0]), n)
        # checks if the hash value already exists as a key
        if h_key in result.keys():
            # adds the record to the existing partition
            result[h_key].add(t)
        else:
            # creates a new partition with the hash value as key
            # adds the record as a tuple
            result[h_key] = {tuple(t)}

    return result


def H(r):
    """
    We define a hash function 'H' that is used in the hashing process works
    by summing the first and second digits of the hashed attribute, which
    in this case is the join attribute.

    Arguments:
    r -- a record where hashing will be applied on its join attribute

    Return:
    result -- the hash index of the record r
    """

    # Convert the value of the join attribute into the digits
    digits = [int(d) for d in str(r[0])]

    # 23 = 2 + 3 = 5

    # Calulate the sum of elemenets in the digits
    # sums the first and second digits of the join attribute of a record
    return sum(digits)


def outer_join(L, R, join="left"):
    """outer join using Hash-based join algorithm"""

    # swaps the input relations L & R to perform a right join instead of a left join
    if join == "right":
        L, R = R, L

    # start inner join
    if join == "inner":
        #  creates a dictionary
        h_dic = {}
        # store the records in R hashed by their join attribute using the hash function
        for r in R:
            h_r = H(r)
            if h_r in h_dic.keys():
                h_dic[h_r].add(r)
            else:
                h_dic[h_r] = {r}

        result = []
        for l in L:
            # hashes its join attribute using H
            h_l = H(l)
            #  If a match is found
            if h_l in h_dic.keys():
                for item in h_dic[h_l]:
                    if item[0] == l[0]:  # prevent collision
                        # appends a three-element list to the result list
                        result.append(l[0] + item[1:])
        return result

    elif join in ["left", "right"]:
        #  creates a dictionary
        h_dic = {}
        for r in R:
            h_r = H(r)
            if h_r in h_dic.keys():
                h_dic[h_r].add(r)
            else:
                h_dic[h_r] = {r}

        result = []

        # it iterates over each record in L (for a left join) or R (for a right join)
        # we already swapped
        for l in L:
            isFound = False  # to check whether there is a match found.
            h_l = H(l)

            if h_l in h_dic.keys():
                for item in h_dic[h_l]:
                    if item[0] == l[0]:  # want to get exact ID match
                        result.append(l + item[1:])
                        isFound = True
            # If no match is found
            # The difference of inner join
            if not isFound:
                result.append(l[1:] + tuple(["None"]))
        return result

    else:
        raise AttributeError("join should be in {left, right, inner}.")


def roja(L, R, n, output_file_path):
    """left outer join using ROJA

    L -- a list of records from Left relation
    R -- a list of records from Right relation
    n -- number of partitions/processors

    """
    # 1st step = distribution using hash partitioning
    tracemalloc.start()
    start_time = time.perf_counter()

    l_dis = hash_distribution(L, n)
    r_dis = hash_distribution(R, n)

    # Apply left outer join for each processor
    pool = mp.Pool(n)
    results = []

    # for each paritition
    for i in l_dis.keys():
        # apply a join on each processor
        result = pool.apply_async(outer_join, [l_dis[i], r_dis[i]])
        results.append(result)

    # Get the results
    output = []
    for x in results:
        output.extend(x.get())

    with open(output_file_path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(output)

    elapsed_time = time.perf_counter() - start_time
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics("lineno")
    total_memory = sum(stat.size for stat in top_stats)

    return elapsed_time, total_memory


def read_csv(filename):
    with open(filename, "r") as f:
        data = f.readlines()[1:]  # skip first header line
    return [tuple(line.strip().split(",")) for line in data]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--R-file", help="R file", required=True)
    parser.add_argument("--S-file", help="S file", required=True)
    parser.add_argument(
        "--concurrency-count",
        help="Number of parallel concurrent run",
        required=False,
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

    elapsed_time, memory_usage = roja(R, S, args.concurrency_count, args.output_file)
    print(f"Memory usage: {memory_usage} bytes or {memory_usage / 1024 / 1024} MB")
    print(f"Elapsed time: {elapsed_time} seconds")
    print("-------------------------")
