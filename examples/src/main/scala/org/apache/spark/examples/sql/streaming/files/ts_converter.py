import pandas as pd
from datetime import datetime, timedelta

# Function to convert milliseconds to the required datetime format
def convert_timestamp(ms):
    # Base date: year 0001, month 01, day 01
    base_date = datetime(1, 1, 1)
    # Convert milliseconds to timedelta
    delta = timedelta(milliseconds=ms)
    # Add timedelta to base date
    return (base_date + delta).isoformat()

# Load the CSV file
input_file = '/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/spark/dataset_io/sample-1000000-inorder.csv'
output_file = '/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/spark/dataset_io/sample-1000000-inorder.csv'

# Read the input CSV into a pandas DataFrame
df = pd.read_csv(input_file)

# Convert the timestamp column
df['ts'] = df['ts'].apply(convert_timestamp)

# Save the modified DataFrame back to a CSV file
df.to_csv(output_file, index=False)

print(f"Modified CSV saved to {output_file}")
