import pandas as pd
import re
from datetime import datetime

INPUT_CSV = "extracted_cases.csv"
OUTPUT_CSV = "extracted_cases_clean.csv"

# Words to remove from judge names
JUDGE_PREFIXES = [
    r"Hon'?ble", r"Justice", r"Mr\.", r"Ms\.", r"Mrs\.", r"Shri", r"Smt\.", r"Dr\."
]

def standardize_date(date_str):
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str or pd.isna(date_str):
        return ""
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%B %d, %Y", "%d %B %Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str.strip()  # return as is if format unknown

def clean_judge_names(name_str):
    """Remove honorifics and extra spaces from judge names."""
    if not name_str or pd.isna(name_str):
        return ""
    cleaned = name_str
    for prefix in JUDGE_PREFIXES:
        cleaned = re.sub(prefix, "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(", ").strip()
    return cleaned

def main():
    df = pd.read_csv(INPUT_CSV)

    # 1️⃣ Date cleaning
    df["Date of Judgment"] = df["Date of Judgment"].apply(standardize_date)

    # 2️⃣ Judge name cleaning
    df["Judges"] = df["Judges"].apply(clean_judge_names)

    # 3️⃣ Fill NaN with empty string
    df = df.fillna("")

    # 4️⃣ Remove duplicates
    df = df.drop_duplicates(subset=["Case Number", "Court Name", "Date of Judgment"])

    # Save cleaned dataset
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Cleaned data saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
