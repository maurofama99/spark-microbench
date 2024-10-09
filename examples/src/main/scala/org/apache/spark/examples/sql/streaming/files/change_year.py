import pandas as pd
from datetime import datetime

# Function to update the year in the timestamp
def update_year_to_2024(ts_str):
    # Check if the timestamp has microseconds
    if '.' in ts_str:
        # Convert the string to a datetime object with microseconds
        ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        # Convert the string to a datetime object without microseconds
        ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")

    # Replace the year with 2024
    ts = ts.replace(year=2024)

    # Convert back to string in the same format (with microseconds if they exist)
    if '.' in ts_str:
        return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")
    else:
        return ts.strftime("%Y-%m-%dT%H:%M:%S")

# Function to read the CSV and update the 'ts' field
def update_csv_year(input_csv, output_csv):
    # Read the CSV file
    df = pd.read_csv(input_csv)

    # Update the 'ts' column
    df['ts'] = df['ts'].apply(update_year_to_2024)

    # Save the modified DataFrame to a new CSV
    df.to_csv(output_csv, index=False)
    print(f"Updated CSV saved to {output_csv}")

# Example usage
input_csv = '/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/spark/dataset_io/sample-1000000-inorder.csv'  # replace with your input file path
output_csv = '/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/spark/dataset_io/sample-1000000-inorder.csv'  # replace with your desired output file path
update_csv_year(input_csv, output_csv)
