import pandas as pd
import os
import numpy as np

# -----------------------------
# 1. SETUP PATHS
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_CLEAN = os.path.join(BASE_DIR, "data_cleaned", "referrals_cleaned.csv")
DATA_RAW = os.path.join(BASE_DIR, "data_raw")

STATUS_CSV = os.path.join(DATA_RAW, "user_referral_statuses.csv")
REWARDS_CSV = os.path.join(DATA_RAW, "referral_rewards.csv")


# -----------------------------
# 2. HELPER: CATEGORY LOGIC
# -----------------------------
def get_source_category(source):
    """Re-creates the category if missing from the CSV"""
    s = str(source).lower()
    if any(x in s for x in ['user sign up', 'app', 'web', 'online']): return 'Online'
    if any(x in s for x in ['draft transaction', 'lead', 'walk']): return 'Offline'
    return 'Other'


def main():
    print("🕵️‍♂️ Starting Fraud Detection & Final Reporting...")

    # 1. Load Data
    if not os.path.exists(DATA_CLEAN):
        print("❌ Error: cleaned data not found.")
        return
    df = pd.read_csv(DATA_CLEAN)

    # --- CRITICAL FIX: Ensure 'referral_source_category' exists ---
    if 'referral_source_category' not in df.columns:
        if 'referral_source' in df.columns:
            print("⚠️ 'referral_source_category' missing. Re-calculating it now...")
            df['referral_source_category'] = df['referral_source'].apply(get_source_category)
        else:
            df['referral_source_category'] = "Unknown"

    # 2. Merge Status Names
    if os.path.exists(STATUS_CSV):
        statuses = pd.read_csv(STATUS_CSV)
        statuses.columns = statuses.columns.str.strip()
        # Rename 'description' -> 'status_name'
        if 'description' in statuses.columns:
            statuses = statuses.rename(columns={'id': 'user_referral_status_id', 'description': 'status_name'})
            df = df.merge(statuses[['user_referral_status_id', 'status_name']], on='user_referral_status_id',
                          how='left')

    if 'status_name' not in df.columns:
        df['status_name'] = 'Unknown'

    # 3. Merge Rewards
    if os.path.exists(REWARDS_CSV):
        rewards = pd.read_csv(REWARDS_CSV)
        rewards.columns = rewards.columns.str.strip()
        if 'user_referral_id' in rewards.columns:
            rewards = rewards.rename(columns={'user_referral_id': 'id'})

        # Match IDs (referral_id usually links to id in rewards)
        join_key = 'referral_id' if 'referral_id' in df.columns else 'id'

        # Ensure join key is same type
        if join_key in df.columns and join_key in rewards.columns:
            # Drop duplicates in rewards to avoid row explosion
            rewards = rewards.drop_duplicates(subset=[join_key])
            df = df.merge(rewards, on=join_key, how='left', suffixes=('', '_reward'))

    # 4. Prepare Logic Columns
    if 'reward_value' not in df.columns: df['reward_value'] = 0
    df['reward_value'] = df['reward_value'].fillna(0)

    # Convert dates
    for col in ['referral_at_local', 'transaction_at_local', 'referrer_expiry']:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce')

    # Logic Flags
    df['has_reward'] = df['reward_value'] > 0
    df['is_success'] = df['status_name'].astype(str).str.contains('Berhasil|Success', case=False, regex=True)
    df['is_pending_or_failed'] = df['status_name'].astype(str).str.contains('Menunggu|Tidak|Fail|Pending', case=False,
                                                                            regex=True)

    df['has_trans_id'] = df['transaction_id'].notna() & (df['transaction_id'].astype(str) != 'nan')
    df['is_trans_paid'] = df['transaction_status'].astype(str).str.upper() == 'PAID'
    df['is_trans_new'] = df['transaction_type'].astype(str).str.upper() == 'NEW'
    df['tx_after_ref'] = df['transaction_at_local'] >= df['referral_at_local']

    # Same Month
    df['same_month'] = False
    mask = df['transaction_at_local'].notna() & df['referral_at_local'].notna()
    if mask.any():
        df.loc[mask, 'same_month'] = (df.loc[mask, 'transaction_at_local'].dt.to_period('M') ==
                                      df.loc[mask, 'referral_at_local'].dt.to_period('M'))

    # User Checks
    now = pd.Timestamp.now().normalize()
    if 'referrer_expiry' in df.columns:
        df['is_member_active'] = df['referrer_expiry'] >= now
    else:
        df['is_member_active'] = False

    if 'referrer_deleted' in df.columns:
        df['is_account_active'] = (df['referrer_deleted'] == False) | (
                    df['referrer_deleted'].astype(str).str.lower() == 'false')
    else:
        df['is_account_active'] = True

    if 'is_reward_granted' in df.columns:
        df['is_granted'] = (df['is_reward_granted'] == True) | (
                    df['is_reward_granted'].astype(str).str.lower() == 'true')
    else:
        df['is_granted'] = False

    # 5. Apply Business Logic
    def check_logic(row):
        # Valid 1: Perfect
        if (row['has_reward'] and row['is_success'] and row['has_trans_id'] and
                row['is_trans_paid'] and row['is_trans_new'] and row['tx_after_ref'] and
                row['same_month'] and row['is_member_active'] and row['is_account_active'] and
                row['is_granted']):
            return True
        # Valid 2: Pending/Failed (No Reward)
        if row['is_pending_or_failed'] and not row['has_reward']:
            return True
        # Invalid Rules
        if row['has_reward'] and not row['is_success']: return False
        if row['has_reward'] and not row['has_trans_id']: return False
        if (not row['has_reward']) and row['has_trans_id'] and row['is_trans_paid'] and row[
            'tx_after_ref']: return False
        if row['is_success'] and not row['has_reward']: return False
        if row['has_trans_id'] and not row['tx_after_ref']: return False

        return False

    print("⚙️ Applying Logic...")
    df['is_business_logic_valid'] = df.apply(check_logic, axis=1)

    # -----------------------------
    # 6. FORMAT FINAL OUTPUT
    # -----------------------------
    print("📝 Formatting Final Report...")

    # Safely get columns, providing defaults if missing
    final_df = pd.DataFrame()

    # ID logic
    if 'id' in df.columns:
        final_df['referral_details_id'] = df['id']
    else:
        final_df['referral_details_id'] = range(101, 101 + len(df))

    final_df['referral_id'] = df['referral_id'] if 'referral_id' in df.columns else df[
        'id'] if 'id' in df.columns else "Unknown"
    final_df['referral_source'] = df['referral_source'] if 'referral_source' in df.columns else "Unknown"

    # This is the line that was crashing - now safe because we fixed it in step 1
    final_df['referral_source_category'] = df['referral_source_category']

    final_df['referral_at'] = df['referral_at_local']

    final_df['referrer_id'] = df['referrer_id']
    final_df['referrer_name'] = df['user_name'] if 'user_name' in df.columns else "Unknown"
    final_df['referrer_phone_number'] = df['phone_number'] if 'phone_number' in df.columns else "Unknown"
    final_df['referrer_homeclub'] = df['home_club'] if 'home_club' in df.columns else "Unknown"

    final_df['referee_id'] = df['referee_id']
    final_df['referee_name'] = df['referee_name'] if 'referee_name' in df.columns else "Unknown"
    final_df['referee_phone'] = df['referee_phone'] if 'referee_phone' in df.columns else "Unknown"

    final_df['is_business_logic_valid'] = df['is_business_logic_valid']

    print(f"📊 Final Row Count: {len(final_df)}")

    # Save
    save_path = os.path.join(BASE_DIR, "data_cleaned", "final_referral_report.csv")
    final_df.to_csv(save_path, index=False)
    print(f"✅ FINAL REPORT SAVED: {save_path}")
    print(final_df[['referral_id', 'referral_source_category', 'is_business_logic_valid']].head())


if __name__ == "__main__":
    main()