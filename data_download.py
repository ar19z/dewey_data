import requests
import os
from tqdm import tqdm
import time
from dotenv import load_dotenv

# 1. Load the .env file
load_dotenv()

# 2. Assign the variables from the .env or define them here
# If you put PROJECT_ID in your .env, use: os.getenv("PROJECT_ID")
# If not, just define it clearly here:
PROJECT_ID = "prj_4xtxbg4c__fldr_i9xb9wa9pmfyrxng9"
API_KEY = os.getenv("DEWEY_API_KEY")


# SMART RELATIVE PATH: Works on any computer
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "dewey_data_downloads")

os.makedirs(SAVE_DIR, exist_ok=True)

def download_with_progress():
    url = f"https://api.deweydata.io/api/v1/external/data/{PROJECT_ID}"
    headers = {"X-API-KEY": API_KEY, "accept": "application/json"}
    
    print(f"Connecting to Dewey API...")
    
    # Retry logic for the initial API call
    max_retries = 5
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            break
        print(f"Attempt {attempt+1} failed (Status {response.status_code}). Retrying in 5s...")
        time.sleep(5)
    else:
        print("Could not connect to API after multiple attempts.")
        return

    files = response.json().get('download_links', [])
    total_files = len(files)
    print(f"Found {total_files} files to download.\n")

    for i, file_info in enumerate(files, 1):
        d_url = file_info['link']
        filename = file_info.get('filename', f"job_postings_part_{i}.csv.gz")
        path = os.path.join(SAVE_DIR, filename)

        # SKIP logic: Don't redownload what we already have
        if os.path.exists(path) and os.path.getsize(path) > 10000:
            print(f"Skipping {filename} (already exists).")
            continue

        # DOWNLOAD logic with individual file retry (handles the 500 error)
        success = False
        while not success:
            try:
                with requests.get(d_url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    
                    description = f"File {i}/{total_files}"
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=description) as pbar:
                        with open(path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=1024*1024):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                success = True
            except Exception as e:
                print(f"\n[!] Error on {filename}: {e}. Retrying in 10s...")
                time.sleep(10)

    print("Download complete.")

if __name__ == "__main__":
    download_with_progress()
