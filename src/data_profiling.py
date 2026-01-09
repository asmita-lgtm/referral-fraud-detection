import pandas as pd
import os


# --- Your Existing Logic ---
def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Generates data profiling metrics for a DataFrame.
    """
    profile = {}
    profile["row_count"] = df.shape[0]
    profile["column_count"] = df.shape[1]
    profile["data_types"] = df.dtypes.astype(str)
    profile["missing_values"] = df.isnull().sum()
    profile["missing_percentage"] = (df.isnull().mean() * 100).round(2)
    profile["duplicate_rows"] = df.duplicated().sum()
    profile["unique_values"] = df.nunique()

    # Handle numeric summary safely
    numeric_df = df.select_dtypes(include=["int64", "float64"])
    if not numeric_df.empty and not numeric_df.columns.empty:
        profile["summary_statistics"] = numeric_df.describe().transpose()
    else:
        profile["summary_statistics"] = pd.DataFrame()

    return profile


def save_profile(profile: dict, file_name: str, output_folder: str) -> None:
    """
    Saves profiling results as multiple CSV files.
    """
    os.makedirs(output_folder, exist_ok=True)
    base_name = file_name.replace(".csv", "")

    # Save individual reports
    profile["data_types"].to_csv(f"{output_folder}/{base_name}_data_types.csv")
    profile["missing_values"].to_csv(f"{output_folder}/{base_name}_missing_values.csv")
    profile["missing_percentage"].to_csv(f"{output_folder}/{base_name}_missing_percentage.csv")
    profile["unique_values"].to_csv(f"{output_folder}/{base_name}_unique_values.csv")

    if not profile["summary_statistics"].empty:
        profile["summary_statistics"].to_csv(f"{output_folder}/{base_name}_summary_statistics.csv")

    print(f"✅ Profiling complete for: {file_name}")


# --- NEW: Execution Logic ---
def main():
    # 1. Setup Paths
    # This logic finds your project root regardless of where you run it from
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_RAW = os.path.join(BASE_DIR, "data_raw")
    DATA_CLEANED = os.path.join(BASE_DIR, "data_cleaned")
    OUTPUT_DIR = os.path.join(BASE_DIR, "data_profile_reports")

    # 2. List of files to profile
    files_to_profile = [
        (DATA_RAW, "user_logs.csv"),
        (DATA_RAW, "user_referral.csv"),
        (DATA_RAW, "referral_status.csv"),  # Including this if you have it
        (DATA_RAW, "transactions.csv"),  # Including this if you have it
        (DATA_CLEANED, "final_referral_report.csv")
    ]

    print("🚀 Starting Data Profiling...\n")

    # 3. Loop through files and profile them
    for folder, filename in files_to_profile:
        file_path = os.path.join(folder, filename)

        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                profile = profile_dataframe(df)
                save_profile(profile, filename, OUTPUT_DIR)
            except Exception as e:
                print(f"⚠️ Error processing {filename}: {e}")
        else:
            print(f"⚠️ Skipped {filename} (File not found in {folder})")

    print(f"\n✅ All reports saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()