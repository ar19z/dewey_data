import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from tqdm import tqdm

# --- CONFIG ---
DATA_DIR = "dewey_data_downloads"
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
input_file = os.path.join(DATA_DIR, csv_files[0])

# Mapping the first 2 digits of NAICS to common Sector Names
NAICS_MAP = {
    '11': 'Agriculture/Forestry', '21': 'Mining/Oil & Gas', '22': 'Utilities',
    '23': 'Construction', '31': 'Manufacturing', '32': 'Manufacturing',
    '33': 'Manufacturing', '42': 'Wholesale Trade', '44': 'Retail Trade',
    '45': 'Retail Trade', '48': 'Transport/Warehouse', '49': 'Transport/Warehouse',
    '51': 'Information (Tech)', '52': 'Finance & Insurance', '53': 'Real Estate',
    '54': 'Prof/Sci/Tech Services', '55': 'Management of Companies',
    '56': 'Admin/Support/Waste', '61': 'Educational Services',
    '62': 'Health Care', '71': 'Arts/Entertain/Rec', '72': 'Accommodation/Food',
    '81': 'Other Services', '92': 'Public Admin'
}


def analyze_salaries():
    chunk_size = 100000
    stats = {}  # {Sector_Name: [sum_salary, count]}

    # Initialize tqdm with total file size for an accurate progress bar
    file_size = os.path.getsize(input_file)
    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing Data")

    try:
        # Use chunking to avoid RAM issues
        for chunk in pd.read_csv(input_file, usecols=['NAICS_PRIMARY', 'SALARY'], chunksize=chunk_size):
            chunk = chunk.dropna(subset=['NAICS_PRIMARY', 'SALARY'])

            # Convert NAICS to string and grab the first 2 digits (The Sector level)
            chunk['Sector_Code'] = chunk['NAICS_PRIMARY'].astype(str).str[:2]
            chunk['Sector_Name'] = chunk['Sector_Code'].map(NAICS_MAP).fillna('Other/Unknown')

            group = chunk.groupby('Sector_Name')['SALARY'].agg(['sum', 'count'])

            for sector, row in group.iterrows():
                if sector not in stats:
                    stats[sector] = [0, 0]
                stats[sector][0] += row['sum']
                stats[sector][1] += row['count']

            # Update bar by the number of bytes processed in this chunk (approximate)
            # A rough estimate for CSV rows is ~150-200 bytes per row
            pbar.update(chunk_size * 200)

        pbar.close()

        # Convert to DataFrame for plotting
        final_data = [{'Sector': k, 'Avg_Salary': v[0] / v[1]} for k, v in stats.items() if v[1] > 100]
        df_results = pd.DataFrame(final_data).sort_values(by='Avg_Salary', ascending=False)

        # Create Visualization
        plt.figure(figsize=(12, 8))
        sns.set_theme(style="whitegrid")
        sns.barplot(data=df_results, x='Avg_Salary', y='Sector', palette='magma')

        plt.title('Average Annual Salary by Industry Sector', fontsize=16, fontweight='bold')
        plt.xlabel('Average Salary ($)', fontsize=12)
        plt.ylabel('Industry Sector', fontsize=12)

        plt.tight_layout()
        plt.savefig("salary_distribution.png")
        print("Success! Chart saved as 'salary_distribution.png'")
        plt.show()

    except Exception as e:
        pbar.close()
        print(f"Error: {e}")


if __name__ == "__main__":
    analyze_salaries()