# Referral Fraud Detection System

## 1. Project Overview
This project is a data engineering solution designed to validate user referral rewards. It processes raw logs, merges transaction data, and applies strict business logic to detect potential fraud or system errors. The final output is a clean, 46-row report (`final_referral_report.csv`) identifying valid and invalid referrals.

## 2. Project Structure
- **`src/`**: Contains the source code.
  - `time.py`: Handles data cleaning and timezone conversion.
  - `fraud_check.py`: The main logic engine that merges data and applies fraud rules.
- **`data_raw/`**: Input CSV files (user logs, transactions, referral statuses).
- **`data_cleaned/`**: Output folder where the final report is saved.
- **`Dockerfile`**: Configuration for containerizing the application.
- **`requirements.txt`**: List of Python dependencies.

## 3. Setup & Running Instructions (Docker)
This project is containerized to ensure consistent execution. The final report is generated inside the container and automatically exported to your local machine.

### Prerequisites
- Docker Desktop must be installed and running.

### Step 1: Build the Image
Open your terminal in the project root folder and run:
```bash
docker build -t referral-app .
docker run -v "${PWD}/data_cleaned:/app/data_cleaned" referral-app
Column Name,Data Type,Description
referral_details_id,Integer,Unique identifier for the referral record.
referral_id,String,The UUID of the specific referral event.
referral_source,String,"The origin of the referral (e.g., ""User Sign Up"")."
referral_source_category,String,"Categorized source: ""Online"" or ""Offline""."
referral_at,DateTime,Timestamp of referral creation (Local Time).
referrer_id,String,UUID of the user who sent the invite.
referrer_name,String,Full name of the referrer.
referrer_phone_number,String,Contact number of the referrer.
referrer_homeclub,String,Home club location of the referrer.
referee_id,String,UUID of the new invited user.
referee_name,String,Full name of the referee.
referee_phone,String,Contact number of the referee.
is_business_logic_valid,Boolean,True = Valid Reward; False = Invalid/Fraud.