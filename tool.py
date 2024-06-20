import pandas as pd
from datetime import datetime


# Function for data cleanup
def clean_data(df, is_open_list=False):
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

    # If it's the open list, also process 'Created Date' and 'Date Comparison'
    if is_open_list:
        df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
        df["Date Comparison"] = pd.to_datetime(df["Date Comparison"], errors="coerce")

    return df


# Function to determine the status
def determine_status(row):
    today = datetime.today()
    closed_date = row["Date Closed"] if pd.notna(row["Date Closed"]) else today

    if row["Implementation Phase"] in ["phase 1", "n/a"]:
        if closed_date > row["Date Comparison"]:
            return "Late"
        else:
            return "Not Late"
    elif row["Implementation Phase"] == "phase 2":
        if row["Any_Blanks_phase 1"]:
            if closed_date > row["Date Comparison"]:
                return "Gated Late"
            else:
                return "Gated Not Late"
        else:
            if closed_date > row["Date Comparison"]:
                return "Late"
            else:
                return "Not Late"
    return None


# Step 1: Read the first CSV file
file_path_all_ca = "./KPI-QE List.csv"  # Replace with your actual file path
try:
    df_all_ca = pd.read_csv(file_path_all_ca)
    print("CSV file 'KPI-QE List' loaded successfully.")
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
file_path_open_list = "./KPI-CA List.csv"  # Replace with your actual file path
try:
    df_open_list = pd.read_csv(file_path_open_list)
    print("CSV file 'KPI-CA List' loaded successfully.")
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
df_open_list = clean_data(df_open_list, is_open_list=True)

# Step 5: Group by 'QE ID' and 'Implementation Phase' and find the maximum 'Date Closed' and presence of blanks
df_all_ca_grouped = (
    df_all_ca.groupby(["QE ID", "Implementation Phase"])
    .agg(
        Latest_Date_Closed=("Date Closed", "max"),
        Any_Blanks=("Date Closed", lambda x: x.isnull().any()),
    )
    .reset_index()
)

# Ensure Any_Blanks is either True or False
df_all_ca_grouped["Any_Blanks"] = df_all_ca_grouped["Any_Blanks"].apply(
    lambda x: bool(x)
)

# Step 6: Pivot the data to make 'Implementation Phase' columns
df_all_ca_pivot = df_all_ca_grouped.pivot(
    index="QE ID",
    columns="Implementation Phase",
    values=["Latest_Date_Closed", "Any_Blanks"],
)

# Flatten the MultiIndex columns
df_all_ca_pivot.columns = [
    "_".join(col).strip() for col in df_all_ca_pivot.columns.values
]

# Fill NaN values in Any_Blanks columns with False
any_blanks_columns = [col for col in df_all_ca_pivot.columns if "Any_Blanks" in col]
df_all_ca_pivot[any_blanks_columns] = df_all_ca_pivot[any_blanks_columns].fillna(False)

# Step 7: Merge the pivoted data with the 'OpenList' data
df_merged = df_open_list.merge(df_all_ca_pivot, on="QE ID", how="left")

# Step 8: Drop specified columns
columns_to_drop = [
    "Last Modified Date",
    "Record ID",
    "Record ID.1",
    "URL-Record ID",
    "URL-Owner",
    "URL-Record ID.1",
]
df_merged.drop(columns=columns_to_drop, inplace=True)

# Step 9: Add custom status column
df_merged["Status"] = df_merged.apply(determine_status, axis=1)

# Step 10: Output the results
print("Merged Data:")
print(df_merged.head())

# Optionally, save the results to a new CSV file
output_file_path = "./merged_output.csv"  # Replace with your actual file path
try:
    df_merged.to_csv(output_file_path, index=False)
    print(f"Results successfully saved to {output_file_path}")
except Exception as e:
    print(f"An error occurred while saving the file: {e}")
