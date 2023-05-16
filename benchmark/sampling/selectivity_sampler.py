import pandas as pd
import numpy as np
import argparse


def generate_selectivity_datasets(left_file, right_file, selectivity_ratios):
    """Generate datasets with varying selectivity ratios.

    Args:
        left_file (str): The file path of the left data file.
        right_file (str): The file path of the right data file.
        selectivity_ratios (list): A list of selectivity ratios to generate datasets for.

    Returns:
        None
    """
    # load the left and right data files
    left_data = pd.read_csv(left_file)
    right_data = pd.read_csv(right_file)

    # define the base dataset as 50% positive and 50% negative
    base_ratio = selectivity_ratios[0]
    base_size = len(left_data) * base_ratio
    # sample the left data to get the base dataset
    base_data = left_data.sample(frac=base_ratio, random_state=42)
    write_to_file(base_ratio, base_data)

    # generate datasets with increasing selectivity ratios
    for ratio in selectivity_ratios[1:]:
        # determine the number of dangling tuples to include
        num_dangling = int(len(left_data) * ratio) - base_size
        # sample data with replacement for row that are in left but not in right (dangling)
        dangling_tuples = left_data[
            ~left_data["movieId"].isin(right_data["movieId"])
        ].sample(int(num_dangling), random_state=42, replace=True)

        # combine the base dataset with the dangling tuples
        combined_data = pd.concat([base_data, dangling_tuples], ignore_index=True)

        write_to_file(ratio, combined_data)


def write_to_file(selectivity_ratio, dataset):
    """Helper function to write a dataset to a CSV file with a specific selectivity ratio.

    Args:
        selectivity_ratio (float): The selectivity ratio associated with the dataset.
        dataset (DataFrame): The dataset to write to the CSV file.

    Returns:
        None
    """
    filename = f"movies_selectivity_{int(selectivity_ratio*100)}.csv"
    dataset[["movieId", "title"]].to_csv(filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate selectivity datasets")
    parser.add_argument(
        "--l", type=str, required=True, help="Path to the left table CSV file"
    )
    parser.add_argument(
        "--r", type=str, required=True, help="Path to the right table CSV file"
    )
    parser.add_argument(
        "--s",
        type=float,
        nargs="+",
        required=True,
        help="Selectivity ratios for generating datasets",
    )

    args = parser.parse_args()
    generate_selectivity_datasets(args.l, args.r, args.s)
