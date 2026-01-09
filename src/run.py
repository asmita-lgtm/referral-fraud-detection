
import os
from ingest import load_all_csv_files
from data_profiling import profile_dataframe, save_profile

# Resolve project base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define input/output folders
RAW_DATA_FOLDER = os.path.join(BASE_DIR, "data_raw")
PROFILE_OUTPUT_FOLDER = os.path.join(BASE_DIR, "data_profile_reports")

def run_data_profiling_pipeline():
    print("Starting data profiling pipeline...")

    # Debugging: show paths and files
    print("BASE_DIR:", BASE_DIR)
    print("RAW_DATA_FOLDER:", RAW_DATA_FOLDER)
    if os.path.exists(RAW_DATA_FOLDER):
        print("Files in RAW_DATA_FOLDER:", os.listdir(RAW_DATA_FOLDER))
    else:
        print("❌ ERROR: RAW_DATA_FOLDER not found at:", RAW_DATA_FOLDER)
        return  # Stop execution if folder missing

    # Ensure output folder exists
    os.makedirs(PROFILE_OUTPUT_FOLDER, exist_ok=True)

    # Load all CSVs
    datasets = load_all_csv_files(RAW_DATA_FOLDER)
    if not datasets:
        print("❌ No datasets loaded. Check your CSV files in:", RAW_DATA_FOLDER)
        return

    # Profile each dataset
    for name, df in datasets.items():
        print(f"Profiling {name}...")
        if df.empty:
            print(f"⚠️ Skipping {name} because DataFrame is empty.")
            continue
        try:
            report = profile_dataframe(df)
            save_profile(report, name, PROFILE_OUTPUT_FOLDER)
            print(f"✅ Saved profile for {name} in {PROFILE_OUTPUT_FOLDER}")
        except Exception as e:
            print(f"❌ Error profiling {name}: {e}")

if __name__ == "__main__":
    run_data_profiling_pipeline()