import pandas as pd
import os

def load_all_csv_files(folder_path):
    print(f"Looking for CSV files in: {folder_path}")

    dataframes = {}

    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            print(f"Loading file: {file}")
            full_path = os.path.join(folder_path, file)
            df = pd.read_csv(full_path)
            dataframes[file] = df

    print(f"Total files loaded: {len(dataframes)}")
    return dataframes