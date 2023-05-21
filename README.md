# SOJA Algorithm for Parallel Outer Join

This repository contains the code for running the SOJA algorithm as well as some benchmarking codes. 

Main implementation uses `multiprocessing` library to simulate a distributed computing environment. See [soja.py](/soja.py)

## Running SOJA

```
python soja.py --R-file benchmark/source/movies.csv --S-file benchmark/source/ratings_1000000.csv --concurrency-count 4
```

Note that, for this POC, the algorithm expects the join attribute to be the first column in both CSV files and are both integers. The output of the parallel join operation can then be seen in `output-soja.csv`. 

## Benchmark

The benchmark test evaluates the SOJA algorithm against ROJA on execution time and memory usasge based on the:
  - Increse in size of left table
  - Increase in selectivity ratio
  - Increase in data skewness (Not tested due to SOJA's prerequisites of balanced partitions)

Refer to `benchmark/` and `benchmark/benchmark.py` folder for implementations

## Running benchmark test

```
make benchmark
```

