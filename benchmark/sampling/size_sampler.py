import pandas as pd
import random
import argparse


def sample_csv(csv_file, sample_size):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Get the total number of rows in the DataFrame
    total_rows = df.shape[0]

    # If the sample size is larger than the total number of rows, set it to the total number of rows
    if sample_size > total_rows:
        sample_size = total_rows

    # Generate a list of random row indices to sample
    sample_indices = random.sample(range(total_rows), sample_size)

    # Use the loc method to extract the random sample from the DataFrame
    sample_df = df.loc[sample_indices]

    # Create a file name for the sampled data with the sample size included
    sample_file = f"{csv_file[:-4]}_{sample_size}.csv"

    # Save the sampled data to a new CSV file
    sample_df.to_csv(sample_file, index=False)

    return sample_df


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="CSV file to sample", required=True)
    parser.add_argument("--sample", help="Sample size", type=int, required=True)
    args = parser.parse_args()

    # Call the sample_csv function with the specified file and sample size
    sample_csv(args.file, args.sample)
