import pandas as pd


# Function for data cleanup
def clean_data(df):
    # Fill blanks in 'Implementation Phase' with 'N/A'
    df["Implementation Phase"] = df["Implementation Phase"].fillna("N/A")

    # Standardize variants of 'N/A'
    na_variants = ["n/.a", "na", "na/"]
    df["Implementation Phase"] = df["Implementation Phase"].replace(
        na_variants, "N/A", regex=True
    )

    # Convert 'Implementation Phase' to lowercase
    df["Implementation Phase"] = df["Implementation Phase"].str.lower()

    # Replace 'phase i' with 'phase 1'
    df["Implementation Phase"] = df["Implementation Phase"].replace(
        "phase i", "phase 1"
    )

    # Truncate 'Implementation Phase' to 7 characters if longer
    df["Implementation Phase"] = df["Implementation Phase"].apply(
        lambda x: x[:7] if len(x) > 7 else x
    )

    # Replace any values not 'n/a', 'phase 1', 'phase 2', or 'phase 3' with 'n/a'
    valid_phases = ["n/a", "phase 1", "phase 2", "phase 3"]
    df["Implementation Phase"] = df["Implementation Phase"].apply(
        lambda x: x if x in valid_phases else "n/a"
    )

    # Convert 'Date Closed' to datetime format
    df["Date Closed"] = pd.to_datetime(df["Date Closed"], errors="coerce")

    return df


# Step 1: Read the first CSV file
file_path_all_ca = "./All CA List.csv"  # Replace with your actual file path
try:
    df_all_ca = pd.read_csv(file_path_all_ca)
    print("CSV file 'All CA List' loaded successfully.")
except FileNotFoundError:
    print(f"Error: The file at {file_path_all_ca} was not found.")
    exit()
except pd.errors.EmptyDataError:
    print("Error: The file is empty.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the file: {e}")
    exit()

# Step 2: Clean the first CSV file data
df_all_ca = clean_data(df_all_ca)

# Step 3: Read the second CSV file
file_path_open_list = "./OpenList.csv"  # Replace with your actual file path
try:
    df_open_list = pd.read_csv(file_path_open_list)
    print("CSV file 'OpenList' loaded successfully.")
except FileNotFoundError:
    print(f"Error: The file at {file_path_open_list} was not found.")
    exit()
except pd.errors.EmptyDataError:
    print("Error: The file is empty.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the file: {e}")
    exit()

# Step 4: Clean the second CSV file data
df_open_list = clean_data(df_open_list)

# Step 5: Group by 'QE ID' and 'Implementation Phase' and find the maximum 'Date Closed'
df_all_ca_grouped = (
    df_all_ca.groupby(["QE ID", "Implementation Phase"])["Date Closed"]
    .max()
    .reset_index()
)
df_all_ca_grouped.rename(columns={"Date Closed": "Latest Date Closed"}, inplace=True)

# Step 6: Pivot the data to make 'Implementation Phase' columns
df_all_ca_pivot = df_all_ca_grouped.pivot(
    index="QE ID", columns="Implementation Phase", values="Latest Date Closed"
)

# Step 7: Merge the pivoted data with the 'OpenList' data
df_merged = df_open_list.merge(df_all_ca_pivot, on="QE ID", how="left")

# Step 8: Output the results
print("Merged Data:")
print(df_merged.head())

# Optionally, save the results to a new CSV file
output_file_path = "./merged_output.csv"  # Replace with your actual file path
try:
    df_merged.to_csv(output_file_path, index=False)
    print(f"Results successfully saved to {output_file_path}")
except Exception as e:
    print(f"An error occurred while saving the file: {e}")
