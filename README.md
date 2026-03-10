# Dewey Job Market Analysis Pipeline

A high-performance Python pipeline designed to download, process, and visualize 32GB+ of job posting data from the Dewey Data API. This project demonstrates big-data handling techniques including chunking, automated decompression, and geospatial visualization.

## Key Features
* Big Data Handling: Processes millions of rows using pandas chunking to maintain a low memory footprint.
* Automated Workflow: Automated download and "Smart Cleanup" unzipping that optimizes disk space by deleting source files after extraction.
* Interactive Mapping: Generates geospatial heatmaps of job density using Folium.
* Economic Insights: Visualizes salary distributions by industry sector (NAICS) and hiring trends over time.

## Tech Stack
* Language: Python 3.12
* Data Science: pandas, seaborn, matplotlib
* Visualization: folium (Leaflet.js)
* Utilities: tqdm (Progress Bars), python-dotenv (Security)

## Installation of PyCharm Academic
For students and researchers, the professional version of PyCharm is available for free through JetBrains' Academic License:
1. Visit the [JetBrains Free Educational Licenses](https://www.jetbrains.com/shop/eform/students) page.
2. Apply using your university (.edu) email address.
3. Once approved, download and install [PyCharm Professional](https://www.jetbrains.com/pycharm/download/?section=mac).
4. Log in to your JetBrains account within the IDE to activate your license.

## Setup and Usage
1. Clone the repository: git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
2. Python Version: Ensure you are using Python 3.12.
3. Install dependencies: pip install -r requirements.txt
4. Configure API: Add your API_KEY and other credentials to a .env file.
5. Run the pipeline: Start with data_download.py followed by unzip_data.py.

## Project Structure
* data_download.py: Securely fetches data from Dewey API.
* unzip_data.py: Handles .gz extraction with auto-cleanup logic.
* map_jobs.py: Generates interactive HTML job density maps.
* salary_analysis.py: Analyzes pay across NAICS industry sectors.
* hiring_trends.py: Tracks posting volume over time.

---
Note: The raw 32GB dataset is ignored by Git to keep the repository lightweight. Run the download scripts locally to generate the data.