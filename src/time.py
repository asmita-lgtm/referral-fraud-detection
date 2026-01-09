import os
import pandas as pd
import pytz

# -----------------------------
# 1. Config & Paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(BASE_DIR, "data_raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data_cleaned")

USERS_CSV = os.path.join(DATA_RAW, "user_logs.csv")
TRANS_CSV = os.path.join(DATA_RAW, "paid_transactions.csv")


# -----------------------------
# 2. Helpers
# -----------------------------
def get_correct_referral_file(directory):
    """Finds the 11KB user_referral.csv and ignores the 1KB placeholder."""
    candidates = []
    # Search for files matching pattern
    if os.path.exists(directory):
        for f in os.listdir(directory):
            if "user_referral" in f and "log" not in f and "status" not in f:
                full_path = os.path.join(directory, f)
                # We prioritize the file that has actual data (size > 2KB)
                if os.path.getsize(full_path) > 2000:
                    candidates.append(full_path)

    if candidates:
        return candidates[0]  # Return the first valid large file
    return None


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


# -----------------------------
# 3. Main Logic
# -----------------------------
def main():
    print("🚀 Starting Master Processing...")

    # 1. LOAD CORRECT REFERRAL FILE
    ref_file = get_correct_referral_file(DATA_RAW)

    # Fallback: if smart loader fails, try exact path from your screenshot
    if not ref_file:
        ref_file = os.path.join(DATA_RAW, "user_referral.csv")

    if not os.path.exists(ref_file):
        print(f"❌ Critical: File not found at {ref_file}")
        return

    print(f"📂 Loading: {os.path.basename(ref_file)}")
    ref = pd.read_csv(ref_file)
    users = pd.read_csv(USERS_CSV)

    # Load Transactions (Critical for Fraud Logic)
    try:
        trans = pd.read_csv(TRANS_CSV)
        print("✅ Transactions loaded.")
    except:
        print("⚠️ Warning: Transactions file not found. Fraud logic will fail later.")
        trans = pd.DataFrame()

        # 2. CLEAN HEADERS & IDs
    ref.columns = ref.columns.str.strip()
    users.columns = users.columns.str.strip()
    if not trans.empty: trans.columns = trans.columns.str.strip()

    # Apply ID Cleaning
    ref['referrer_id'] = ref['referrer_id'].apply(clean_id)
    ref['referee_id'] = ref['referee_id'].apply(clean_id)
    users['user_id'] = users['user_id'].apply(clean_id)

    # Transaction IDs
    if 'transaction_id' in ref.columns:
        ref['transaction_id'] = ref['transaction_id'].apply(clean_id)
    if not trans.empty and 'transaction_id' in trans.columns:
        trans['transaction_id'] = trans['transaction_id'].apply(clean_id)

    # 3. MERGE TIMEZONES & USER DATA
    # We grab timezone + membership info for the fraud check
    tz_map = users[['user_id', 'timezone_homeclub', 'membership_expired_date', 'is_deleted']].dropna(
        subset=['user_id']).drop_duplicates(subset=['user_id'])

    # Merge for Referrer
    ref = ref.merge(tz_map, left_on='referrer_id', right_on='user_id', how='left')
    ref = ref.rename(columns={'timezone_homeclub': 'referrer_tz', 'membership_expired_date': 'referrer_expiry',
                              'is_deleted': 'referrer_deleted'})

    # Merge for Referee
    ref = ref.merge(tz_map, left_on='referee_id', right_on='user_id', how='left', suffixes=('', '_ref'))
    ref = ref.rename(columns={'timezone_homeclub': 'referee_tz'})

    # Fill Timezone Gaps
    ref['final_tz'] = ref['referrer_tz'].fillna(ref['referee_tz'])

    # 4. MERGE TRANSACTIONS
    if not trans.empty and 'transaction_id' in ref.columns:
        ref = ref.merge(trans, on='transaction_id', how='left', suffixes=('', '_trans'))
        print("✅ Merged Transactions data.")

    # 5. TIME CONVERSION
    ref['referral_at'] = pd.to_datetime(ref['referral_at'], utc=True)
    ref['referral_at_local'] = [to_local(ts, tz) for ts, tz in zip(ref['referral_at'], ref['final_tz'])]

    if 'transaction_at' in ref.columns:
        ref['transaction_at'] = pd.to_datetime(ref['transaction_at'], utc=True)
        ref['transaction_at_local'] = [to_local(ts, tz) for ts, tz in zip(ref['transaction_at'], ref['final_tz'])]

    # 6. INITCAP STRINGS (CRITICAL FIX)
    # This replaces the line that was crashing your code
    print("🧹 Cleaning text columns...")
    for col in ref.columns:
        # Only process columns that look like text (object)
        if ref[col].dtype == 'object':
            # Skip IDs, Dates, and Club Names
            if not any(x in col.lower() for x in ['id', 'date', 'club', 'at', 'tz']):
                # Safe Apply: Checks if value is actually a string before capitalizing
                ref[col] = ref[col].apply(lambda x: str(x).title() if pd.notna(x) and isinstance(x, str) else x)

    # 7. SAVE OUTPUT
    os.makedirs(CLEAN_DIR, exist_ok=True)
    save_path = os.path.join(CLEAN_DIR, "referrals_cleaned.csv")

    # Final filter: Drop rows where we completely failed to find a timezone
    #ref = ref.dropna(subset=['referral_at_local'])

    ref.to_csv(save_path, index=False)
    print(f"\n✅ SUCCESS! Data saved to: {save_path}")
    print("Columns available for Fraud Check:", list(ref.columns))


if __name__ == "__main__":
    main()
