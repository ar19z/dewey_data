import gzip
import shutil
import os
from tqdm import tqdm

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "dewey_data_downloads")


def unzip_and_smart_cleanup():
    # 1. Get a list of all .gz files in the folder
    if not os.path.exists(DATA_DIR):
        print(f"Error: Folder {DATA_DIR} does not exist.")
        return

    gz_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.gz')]

    if not gz_files:
        print(f"No .gz files found in {DATA_DIR}. Everything is likely already unzipped!")
        return

    print(f"Found {len(gz_files)} compressed files. Starting smart cleanup...")

    for filename in gz_files:
        gz_path = os.path.join(DATA_DIR, filename)
        csv_path = os.path.join(DATA_DIR, filename[:-3])  # Removes '.gz'

        # --- STEP 1: CHECK IF ALREADY UNZIPPED ---
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 10000:
            print(f"Found existing CSV for {filename}. Deleting redundant .gz file...")
            os.remove(gz_path)
            continue

        # --- STEP 2: DECOMPRESS IF CSV IS MISSING ---
        print(f"Unzipping {filename}...")
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(csv_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Verify the new CSV is healthy before deleting the source
            if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
                os.remove(gz_path)
                print(f"Done: {os.path.basename(csv_path)} created and .gz removed.")

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            # If it failed mid-way, remove the corrupted CSV so we can try again later
            if os.path.exists(csv_path):
                os.remove(csv_path)

    print("Storage optimization complete. Only CSV files remain.")


if __name__ == "__main__":
    unzip_and_smart_cleanup()