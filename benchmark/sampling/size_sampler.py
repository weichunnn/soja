import pandas as pd
import random
import argparse


def sample_csv(csv_file, sample_size):
    """Sample a CSV file and save the sampled data to a new file.

    Args:
        csv_file (str): The file path of the CSV file to sample.
        sample_size (int): The number of rows to sample from the CSV file.

    Returns:
        dataFrame: The sampled data as a Pandas DataFrame.
    """
    df = pd.read_csv(csv_file)
    total_rows = df.shape[0]

    # if the sample size is larger than the total number of rows, set it to the total number of rows
    if sample_size > total_rows:
        sample_size = total_rows

    # generate a list of random row indices to sample
    sample_indices = random.sample(range(total_rows), sample_size)
    # extract the rows with the sampled indices
    sample_df = df.loc[sample_indices]
    sample_file = f"{csv_file[:-4]}_{sample_size}.csv"

    # save the sampled data to a new CSV file
    sample_df.to_csv(sample_file, index=False)
    return sample_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="CSV file to sample", required=True)
    parser.add_argument("--sample", help="Sample size", type=int, required=True)
    args = parser.parse_args()

    sample_csv(args.file, args.sample)
