import subprocess

# test parameters for movies dataset
table_sizes = [10000, 20000, 30000, 40000, 50000]
selectivity_ratio = [50, 60, 70, 80, 90, 100]

ratings_file_path = "source/ratings_8000000.csv"
algo_types = ["roja", "soja"]

if ratings_file_path.endswith(".zip"):
    throw("ratings file is in zip format, please unzip it first")

for i, size in enumerate(table_sizes):
    for algo_type in algo_types:
        command = [
            "python",
            f"../{algo_type}.py",
            "--R-file",
            f"size/movies_{size}.csv",
            "--S-file",
            ratings_file_path,
        ]
        print(f"Running {algo_type.upper()} program with {size} rows in table R")
        subprocess.check_call(command)

        # keep last record for checking
        if i == len(file_sizes) - 1:
            open(f"output-{algo_type}.csv", "w").close()


for i, ratio in enumerate(selectivity_ratio):
    for algo_type in algo_types:
        command = [
            "python",
            f"../{algo_type}.py",
            "--R-file",
            f"selectivity/movies_selectivity_{ratio}.csv",
            "--S-file",
            ratings_file_path,
        ]
        print(
            f"Running {algo_type.upper()} program with {ratio}% selectivity ratio"
        )
        subprocess.check_call(command)

        # keep last record for checking
        if i == len(file_sizes) - 1:
            open(f"output-{algo_type}.csv", "w").close()
