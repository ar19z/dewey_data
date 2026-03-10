import os
import csv

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "dewey_data_downloads")


def peek_at_csv():
    # 1. Find the first .csv file in the folder
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

    if not csv_files:
        print(f"No .csv files found in {DATA_DIR}. Did you unzip them yet?")
        return

    first_file = os.path.join(DATA_DIR, csv_files[0])
    print(f"--- Peeking at: {csv_files[0]} ---\n")

    # 2. Read only the first 3 rows (Header + 2 rows of data)
    try:
        with open(first_file, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 3:  # Stop after 3 rows (0, 1, 2)
                    break
                # Join the list into a string separated by tabs for readability
                print(f"Row {i}: {' | '.join(row)}")
                print("-" * 20)

    except Exception as e:
        print(f"Error reading file: {e}")


if __name__ == "__main__":
    peek_at_csv()