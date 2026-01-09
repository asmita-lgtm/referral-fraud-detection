import os
import pandas as pd
import pytz

# -----------------------------
# 1. Config & Paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(BASE_DIR, "data_raw")
USERS_CSV = os.path.join(DATA_RAW, "user_logs.csv")


# -----------------------------
# 2. HELPER: FIND THE RIGHT FILE
# -----------------------------
def get_correct_referral_file(directory):
    """Finds the largest 'user_referral' file to avoid loading the empty 1KB one."""
    candidates = []
    for f in os.listdir(directory):
        if "user_referral" in f and "log" not in f and "status" not in f:
            full_path = os.path.join(directory, f)
            size = os.path.getsize(full_path)
            candidates.append((full_path, size))

    # Sort by size (Largest first) -> This picks the 11KB file over the 1KB file
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)

    best_file, best_size = candidates[0]
    print(f"📂 SMART LOAD: Selected '{os.path.basename(best_file)}' (Size: {best_size} bytes)")
    return best_file


# -----------------------------
# 3. Helpers
# -----------------------------
def to_local(ts, tz):
    if pd.isna(ts) or pd.isna(tz) or str(tz).lower() == 'nan': return pd.NaT
    try:
        if ts.tzinfo is None: ts = ts.tz_localize("UTC")
        return ts.astimezone(pytz.timezone(str(tz))).replace(tzinfo=None)
    except:
        return ts.replace(tzinfo=None)


def clean_id(val):
    if pd.isna(val) or str(val).lower() in ['nan', 'null', '']: return pd.NA
    val = str(val).strip()
    if val.endswith('.0'): return val[:-2]
    return val


def get_source_category(source):
    s = str(source).lower()
    if any(x in s for x in ['user sign up', 'app', 'web', 'online']): return 'Online'
    if any(x in s for x in ['draft transaction', 'lead', 'walk']): return 'Offline'
    return 'Other'


# -----------------------------
# 4. Main Execution
# -----------------------------
def main():
    print("🚀 Starting Smart Process...")

    # 1. FIND CORRECT FILE
    ref_file = get_correct_referral_file(DATA_RAW)
    if not ref_file:
        print("❌ Critical: No referral file found.")
        return

    # 2. LOAD DATA
    ref = pd.read_csv(ref_file)
    users = pd.read_csv(USERS_CSV)

    # 3. CLEAN HEADERS (Fixes 'Invisible Space' KeyErrors)
    ref.columns = ref.columns.str.strip()
    users.columns = users.columns.str.strip()

    print(f"DEBUG: Columns in loaded file: {list(ref.columns)}")

    # 4. VERIFY COLUMNS
    if 'referee_id' not in ref.columns:
        print("❌ ERROR: Still missing 'referee_id'. Please delete the 1KB 'user_referral' file from your folder.")
        return

    # 5. CLEAN IDs
    ref['referrer_id'] = ref['referrer_id'].apply(clean_id)
    ref['referee_id'] = ref['referee_id'].apply(clean_id)
    users['user_id'] = users['user_id'].apply(clean_id)

    # 6. MERGE TIMEZONES
    tz_map = users[['user_id', 'timezone_homeclub']].dropna().drop_duplicates()

    ref = ref.merge(tz_map, left_on='referrer_id', right_on='user_id', how='left')
    ref = ref.rename(columns={'timezone_homeclub': 'referrer_tz'})

    ref = ref.merge(tz_map, left_on='referee_id', right_on='user_id', how='left', suffixes=('', '_ref'))
    ref = ref.rename(columns={'timezone_homeclub': 'referee_tz'})

    # 7. PROCESS
    ref['final_tz'] = ref['referrer_tz'].fillna(ref['referee_tz'])

    ref['referral_at'] = pd.to_datetime(ref['referral_at'], utc=True)
    ref['referral_at_local'] = [to_local(ts, tz) for ts, tz in zip(ref['referral_at'], ref['final_tz'])]

    # Initcap
    for col in ref.select_dtypes(include=['object']).columns:
        if 'id' not in col.lower() and 'club' not in col.lower():
            ref[col] = ref[col].str.title()

    if 'referral_source' in ref.columns:
        ref['referral_source_category'] = ref['referral_source'].apply(get_source_category)

    ref = ref.dropna(subset=['referral_at_local'])

    print("\n✅ SUCCESS! Sample Data:")
    print(ref[['referee_id', 'referral_at_local', 'referral_source_category']].head())


if __name__ == "__main__":
    main()
