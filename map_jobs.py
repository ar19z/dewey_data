import pandas as pd
import folium
from folium.plugins import HeatMap
import os
from tqdm import tqdm

# --- CONFIG ---
DATA_DIR = "dewey_data_downloads"
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
input_file = os.path.join(DATA_DIR, csv_files[0])


def create_job_heatmap():
    # 1. Get total lines for the progress bar (Estimation is faster than counting)
    file_size = os.path.getsize(input_file)
    print(f"Processing {os.path.basename(input_file)} ({file_size / 1e9:.2f} GB)...")

    # 2. Setup Chunking
    chunk_size = 100000  # 100k rows at a time
    all_points = []

    # We estimate total chunks for the tqdm bar
    # 32GB file / ~500 bytes per row = ~64 million rows.
    # Let's just use a simple counter if total is unknown.

    cols = ['LATITUDE', 'LONGITUDE', 'STATE']

    # Initialize the Progress Bar
    # Since we don't know the exact row count without reading the whole file,
    # we'll use a manual update bar.
    pbar = tqdm(desc="Reading Chunks", unit="rows")

    try:
        # 3. Read in chunks
        for chunk in pd.read_csv(input_file, usecols=cols, chunksize=chunk_size):
            # Filter for FL and drop empty coordinates
            filtered = chunk[chunk['STATE'] == 'FL'].dropna(subset=['LATITUDE', 'LONGITUDE'])

            # Extract coordinates
            points = filtered[['LATITUDE', 'LONGITUDE']].values.tolist()
            all_points.extend(points)

            pbar.update(len(chunk))

        pbar.close()

        if not all_points:
            print("No data found for Florida.")
            return

        print(f"Collected {len(all_points)} data points. Generating map...")

        # 4. Downsample for the Map (Folium struggles with > 50,000 points)
        # We take every 10th or 100th point to keep the heat accurate but the map fast
        final_points = all_points[::10]

        # 5. Create Map
        m = folium.Map(location=[27.6648, -81.5158], zoom_start=7, tiles="CartoDB Positron")
        HeatMap(final_points, radius=8, blur=12).add_to(m)

        m.save("job_density_map.html")
        print("Map saved to job_density_map.html")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    create_job_heatmap()