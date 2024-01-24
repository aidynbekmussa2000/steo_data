import os
import requests
import pandas as pd
import glob
from tqdm import tqdm


API_URL = "https://api.eia.gov/v2/steo/data/"
API_KEY = os.getenv("STEO_API_KEY")


def get_steop_data(series_id: str):
    """
    Get STEO data for a given series ID
    """
    params = {
        "frequency": "monthly",
        "api_key": API_KEY,
        "data[0]": "value",
        "facets[seriesId][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
    }
    
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def clean_description(description: str):
    """
    Clean the description of a series ID
    """
    bad_words = [
        'Crude Oil Production',
        'Crude Oil and Liquid Fuels Supply',
        'Liquid Fuels Consumption',
    ]
    for word in bad_words:
        description = description.replace(word, '')
    description = description.strip().replace('  ', ' ').replace(', ', ',')
    description = description.rstrip(',').lstrip(',')
    return description


def main():
    """
    Get STEO data for all series IDs in series_ids.txt
    """
    series_ids_path = "series_ids.txt"
    tables_dir = "tables"
    excels_dir = "excels"
    excel_file_name = "STEO.xlsx"
    
    with open(series_ids_path, "r") as f:
        series_ids = f.read().splitlines()

    for series_id in tqdm(series_ids):
        table = series_id.split("_")[0]
        region = series_id.split("_")[1]
        path = os.path.join("tables", table)
        os.makedirs(path, exist_ok=True)
        data = get_steop_data(series_id)
        if data:
            df = pd.DataFrame(data["response"]["data"])
            df_path = os.path.join(path, region + ".csv")
            df.to_csv(df_path, index=False)
        else:
            print(f"Failed to retrieve data for {series_id}.")
            
    table_paths = glob.glob(os.path.join(tables_dir, '*'))
    os.makedirs(excels_dir, exist_ok=True)

    excel_path = os.path.join(excels_dir, excel_file_name)
    writer = pd.ExcelWriter(excel_path)

    for table_path in tqdm(table_paths, desc="Writing to Excel File"):
        table = os.path.basename(table_path)
        df = pd.DataFrame()
        regions = glob.glob(os.path.join(table_path, '*.csv'))
        for region in regions:
            df_region = pd.read_csv(region, index_col='period')
            description = df_region['seriesDescription'].iloc[0]
            description = clean_description(description)
            df_region = df_region.rename(columns={'value': description})
            df_region = df_region[description]
            df = pd.concat([df, df_region], axis=1)

        df = df.sort_index(ascending=False)
        df.to_excel(writer, sheet_name=table)

    writer.close()
    print('DataFrames are written successfully to Excel File.')


if __name__ == "__main__":
    main()