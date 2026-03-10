import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from tqdm import tqdm

# --- CONFIG ---
DATA_DIR = "dewey_data_downloads"
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
input_file = os.path.join(DATA_DIR, csv_files[0])


def analyze_hiring_trends():
    chunk_size = 150000
    date_counts = {}

    file_size = os.path.getsize(input_file)
    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Analyzing Timeline")

    try:
        # 1. Stream the data
        for chunk in pd.read_csv(input_file, usecols=['POST_DATE'], chunksize=chunk_size):
            # Convert to datetime, errors='coerce' turns bad dates into NaT (Not a Time)
            chunk['POST_DATE'] = pd.to_datetime(chunk['POST_DATE'], errors='coerce')
            chunk = chunk.dropna(subset=['POST_DATE'])

            # Group by Year-Month (e.g., "2024-05")
            chunk['YearMonth'] = chunk['POST_DATE'].dt.to_period('M')

            counts = chunk['YearMonth'].value_counts()
            for period, count in counts.items():
                date_counts[period] = date_counts.get(period, 0) + count

            pbar.update(chunk_size * 150)  # Approx bytes per chunk

        pbar.close()

        # 2. Prepare Data for Plotting
        df_timeline = pd.DataFrame(list(date_counts.items()), columns=['Month', 'Job_Postings'])
        df_timeline = df_timeline.sort_values('Month')

        # Convert Period back to timestamp for Matplotlib
        df_timeline['Month'] = df_timeline['Month'].dt.to_timestamp()

        # 3. Create the Visualization
        plt.figure(figsize=(14, 7))
        sns.set_theme(style="darkgrid")

        sns.lineplot(data=df_timeline, x='Month', y='Job_Postings', marker='o', color='#2ca02c', linewidth=2.5)

        plt.title('Hiring Trends: Total Job Postings Over Time', fontsize=18, fontweight='bold')
        plt.xlabel('Date (Month)', fontsize=12)
        plt.ylabel('Number of New Postings', fontsize=12)
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig("hiring_timeline.png")
        print(f"\n✅ Timeline saved as 'hiring_timeline.png'")
        plt.show()

    except Exception as e:
        if 'pbar' in locals(): pbar.close()
        print(f"Error: {e}")


if __name__ == "__main__":
    analyze_hiring_trends()