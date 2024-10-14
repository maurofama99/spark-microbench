import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random timestamps within a range
def generate_random_timestamp(start, end):
    time_diff = end - start
    random_seconds = random.randint(0, int(time_diff.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# Function to generate the CSV file
def generate_csv(file_name, total_rows, start_date, end_date):
    data = []

    # Convert input dates to datetime objects
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    for _ in range(total_rows):
        # Generate a random timestamp
        random_ts = generate_random_timestamp(start_datetime, end_datetime)

        # Append row with key=0 and value=2000
        data.append([random_ts.strftime("%Y-%m-%d %H:%M:%S"), 0, 2000])

    # Create a DataFrame
    df = pd.DataFrame(data, columns=['ts', 'key', 'value'])

    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"CSV file '{file_name}' generated with {total_rows} rows.")

# Example usage
file_name = '/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/spark/examples/sql/streaming/files/csv_session4/4kk.csv'  # Replace with desired file name
total_rows = 40000000  # Replace with the total number of rows to generate
start_date = "2024-01-01 00:00:00"  # Replace with the start of the timestamp range
end_date = "2024-03-16 00:00:00"  # Replace with the end of the timestamp range

generate_csv(file_name, total_rows, start_date, end_date)
