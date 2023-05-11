import subprocess

file_sizes = [100000, 500000, 1000000]
algo_types = ['roja', 'soja']

for i, size in enumerate(file_sizes):
  for algo_type in algo_types:
    command = ["python", f"../{algo_type}.py", "--R-file", "source/movies.csv", "--S-file",  f"source/ratings_{size}.csv"]
    print(f"Running {algo_type.upper()} program with file size {size}")
    subprocess.check_call(command)

    # keep last record
    if i == len(file_sizes) - 1:
      open(f"output-{algo_type}.csv", "w").close() 
  
   