import pandas as pd

# Function to return the first X rows from a CSV file and save to a new file
def get_first_x_rows(csv_file, X, output_file):
    # Read the CSV into a DataFrame
    df = pd.read_csv(csv_file)

    # Get the total number of rows in the CSV
    total_rows = len(df)

    # Check if X is less than the total number of rows
    if X > total_rows:
        raise ValueError(f"X is greater than the total number of rows ({total_rows}). Please choose a smaller number.")

    # Get the first X rows
    df_subset = df.head(X)

    # Save the result to a new CSV file
    df_subset.to_csv(output_file, index=False)
    print(f"The first {X} rows have been saved to '{output_file}'.")

# Example usage
csv_file = '/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/spark/examples/sql/streaming/files/csv_session3/3kk.csv'  # Replace with your input CSV file
X = 2500000  # Replace with the number of rows you want
output_file = '/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/spark/examples/sql/streaming/files/csv_session25/2kk5k.csv'  # Replace with the desired output file name

try:
    get_first_x_rows(csv_file, X, output_file)
except ValueError as e:
    print(e)
