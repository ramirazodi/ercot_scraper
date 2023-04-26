# ercot_scraper
ERCOT MIS extract scraper for zip extracts that containt csv files

# README

## ERCOT MIS Data Scraper

This Python script is designed to scrape the latest data available from the ERCOT MIS (Electric Reliability Council of Texas Market Information System) and store the data in individual Pandas DataFrames. The script is configured to handle specific ERCOT MIS report links and process the data.

### Prerequisites

- Python 3.x
- Google Chrome Driver
- Required Python packages listed in the `requirements.txt` file.

### Installing Dependencies

Install the required Python packages using the following command:
pip install -r requirements.txt


### Usage

Update the urls dictionary with the desired ERCOT MIS report links and custom names.
Set the correct path for the Google Chrome Driver in the webdriver_path variable.
Run the script using the following command:
python ercot_mis_scraper.py

This script will download the latest data available from the provided ERCOT MIS report links, process the data, and store it in individual Pandas DataFrames. A summary report of the files processed and download locations will be displayed at the end.
