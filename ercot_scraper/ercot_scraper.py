import pandas as pd
import datetime
import os
import shutil
import zipfile
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException


# This code currently can only find and dowanload the latest zip file in ERCOT links.
# The code returns empty dataframes if links are not .zip files are if decode issues happen
# Only reports from ERCOT MIS can be scraped: https://www.ercot.com/mp/data-products

# ERCOT MIS Report links and custom name - Enter as key-value pairs in dictionary below
urls = {
    'Demand': 'https://www.ercot.com/mp/data-products/data-product-details?id=NP3-560-CD',
    'Solar': 'https://www.ercot.com/mp/data-products/data-product-details?id=NP4-745-CD',
    'Wind': 'https://www.ercot.com/mp/data-products/data-product-details?id=NP4-732-CD',
    'Reserves': 'https://www.ercot.com/mp/data-products/data-product-details?id=NP3-763-CD',
    'Outages': 'https://www.ercot.com/mp/data-products/data-product-details?id=NP3-233-CD',
}

# Install Google Chrome driver from this link: https://chromedriver.chromium.org/downloads
# Google Chrome driver path - relative path from Roozbeh's local macbook below
webdriver_path = '/chromedriver_mac64/chromedriver'


# Below are custom exception classes for exceptions and error handling
class DataFetchingError(Exception):
    pass


class DataProcessingError(Exception):
    pass


class MissingDependencyError(Exception):
    pass


def get_folder_path(folder_name, use_absolute_path=True):
    """
        Returns the folder path using absolute (default) or relative path.
        Parameters:
            folder_name (str): The name of the folder.
            use_absolute_path (bool): If True, uses absolute path. If False, uses relative path.
        Returns:
            str: The path of the folder.
        """
    if use_absolute_path:
        return os.path.expanduser(os.path.join("~", folder_name))
    else:
        return folder_name


def clear_unzip_folder(folder_name):
    """
        Clears the content of the unzip folders (flush and fill).
        Parameters:
            folder_name (str): The name of the folder to be cleared.
        """
    unzip_folder = os.path.expanduser(os.path.join("~", folder_name))
    if os.path.exists(unzip_folder):
        shutil.rmtree(unzip_folder)
    os.makedirs(unzip_folder)

# The function below is currently not used in this code
def get_html_content(url):
    """
        Returns HTML content if no reliance on JavaScript needed.
        Parameters:
            url (str): The URL of the HTML content to be retrieved.
        Returns:
            str: The HTML content of the URL.
        """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise DataFetchingError(f"Error fetching HTML content: {e}")


def get_rendered_html(url, webdriver_path):
    """
       Uses Selenium to get the JavaScript-rendered content from the ERCOT URLs.
       Parameters:
            url (str): The URL of the HTML content to be retrieved.
            webdriver_path (str):The path of the webdriver for Google Chrome.
       Returns:
            str: The JavaScript-rendered content of the URL.
       """
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")

        service = Service(executable_path=webdriver_path)
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except WebDriverException as wde:
            raise MissingDependencyError(
                "ChromeDriver is missing or not set up properly. "
                "Please install and configure it correctly.")
        driver.get(url)
        html = driver.page_source
        driver.quit()
        return html
    except Exception as e:
        raise DataFetchingError(f"Error fetching rendered HTML: {e}")


def get_csv_file_name(soup):
    """
        Extracts the csv filename from the html content.
        Args:
            soup: BeautifulSoup object representing the html content.
        Returns:
            csv_file_name: string representing the csv file name.
        """
    try:
        csv_file_name = (soup.find('table', {'id': 'reportTable'})
                         .find('td', {'class': 'name'})
                         .text.strip())
        return csv_file_name
    except Exception as e:
        raise DataProcessingError(f"Error extracting CSV file name: {e}")


def get_most_recent_link(rows):
    """
        Selects the most recent ERCOT extract to download.
        Args:
            rows: list of rows in the html table representing the report data.
        Returns:
            most_recent_link: string representing the link to the most recent extract.
            most_recent_posted: datetime object representing the time the most recent
            extract was posted.
            file_list: list of strings representing all posted extracts, for reference.
        """
    file_list = list()
    try:
        most_recent_posted = datetime.datetime.min
        most_recent_link = None
        for row in rows:
            posted_str = ' '.join(td.text.strip() for td in row.find_all('td')[1:3])
            posted_dt = datetime.datetime.strptime(posted_str, '%m/%d/%Y %I:%M:%S %p')
            file_list.append(posted_str)
            if posted_dt > most_recent_posted:
                most_recent_posted = posted_dt
                most_recent_link = row.find('a')['href']
        return most_recent_link, most_recent_posted, file_list
    except Exception as e:
        raise DataProcessingError(f"Error finding the most recent link: {e}")


def process_url(url_key, url_value):
    """
      Processes a single ERCOT MIS report URL.
      Args:
          url_key: string representing the key of the URL in the urls dictionary.
          url_value: string representing the value of the URL in the urls dictionary.
      Returns:
          df: pandas dataframe containing the data from the report.
          zip_filename: string representing the name of the downloaded zip file.
          unzip_folder: string representing the path to the folder containing the unzipped files.
          csv_file_name: string representing the name of the CSV file contained in the zip file.
      """
    try:
        html = get_rendered_html(url_value, webdriver_path)

        soup = BeautifulSoup(html, 'html.parser')
        csv_file_name = get_csv_file_name(soup)

        rows = (soup.find('table', {'id': 'reportTable'})
                .find_all('tr')[1:])

        most_recent_link, most_recent_posted, file_list = get_most_recent_link(rows)

        zip_response = requests.get(most_recent_link)

        downloads_folder = get_folder_path("downloads")
        unzip_folder_name = f"downloads/unzipped_files/{url_key}"
        unzip_folder = get_folder_path(unzip_folder_name, use_absolute_path=True)

        # Clear content of the Unzip folders (flush and fill):
        clear_unzip_folder(unzip_folder)

        os.makedirs(downloads_folder, exist_ok=True)
        os.makedirs(unzip_folder, exist_ok=True)

        zip_filename = (os.path.join(downloads_folder,
                                     f'{csv_file_name}_'
                                     f'{most_recent_posted.strftime("%Y%m%d_%H%M%S")}.zip'))

        with open(zip_filename, 'wb') as f:
            f.write(zip_response.content)

        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)

        # Unzipped files for {url_key} are stored in {unzip_folder}
        csv_file = os.path.join(unzip_folder, os.listdir(unzip_folder)[0])
        df = pd.read_csv(csv_file)
        df['report_name'] = url_key

        return df, zip_filename, unzip_folder, csv_file_name

    except DataFetchingError as dfe:
        print(f"DataFetchingError for {url_key}: {dfe}")
    except DataProcessingError as dpe:
        print(f"DataProcessingError for {url_key}: {dpe}")
    except MissingDependencyError as mde:
        print(f"MissingDependencyError for {url_key}: {mde}")
    except Exception as e:
        print(f"Unexpected error occurred for {url_key}: {e}")

    print(f"Returning an empty DataFrame for {url_key}")
    empty_df = pd.DataFrame()
    empty_df['report_name'] = url_key
    return empty_df, None, None, None


def main():
    dataframes = dict()         # Will contain raw data from each extract scraped
    report = dict()             # Will be used to generate a report of files processed
    progress_bar = tqdm(urls.items(), position=0, desc="Processing URLs", leave=True,
                        bar_format="{desc}: {percentage:3.0f}%|{bar:50}{r_bar}")

    for url_key, url_value in progress_bar:
        # Dictionary of dataframes. Each dictionary value can be landed as a raw table later
        dataframes[url_key] = process_url(url_key, url_value)[0]
        report[url_key] = process_url(url_key, url_value)[1:]
        progress_bar.set_description(f"Processing {url_key}...")

    progress_bar.close()

    # Generate summary report of tasks and files processed
    print('Summary of files processed and download locations')
    for i, (key, value) in enumerate(report.items()):
        print(f'Extract #{i} - {key}:')
        print(f'{value}')

if __name__ == "__main__":
    main()
