import os
import glob
import csv
import json
import re
import time
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Gemini
# genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
genai.configure(api_key="")

# Input & output paths
INPUT_DIR = "scrappedText"
OUTPUT_CSV = "extracted_cases.csv"

# CSV columns
columns = [
    "File Name", "Case Title", "Court Name", "Date of Judgment", "Case Number",
    "Judges", "Petitioner(s)", "Respondent(s)", "Legal Issues",
    "Decision Summary", "Outcome", "Citations"
]

def chunk_text(text, chunk_size=50000):
    """Split text into smaller chunks."""
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def call_gemini(prompt, retries=3):
    """Call Gemini API with retries."""
    model = genai.GenerativeModel("gemini-2.0-flash-lite")
    for attempt in range(retries):
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            print(f"⚠ API call failed (attempt {attempt+1}): {e}")
            time.sleep(2)
    return ""

def extract_case_info(text):
    """Extract structured case info using Gemini with chunking."""
    chunks = chunk_text(text)
    merged_data = {key: "" for key in columns[1:]}  # empty fields initially

    for i, chunk in enumerate(chunks):
        print(f"📄 Processing chunk {i+1}/{len(chunks)}...")
        prompt = f"""
        You are a legal document parser.
        Output ONLY valid JSON with these keys:
        Case Title, Court Name, Date of Judgment, Case Number, Judges,
        Petitioner(s), Respondent(s), Legal Issues, Decision Summary, Outcome, Citations.

        If any field is missing, return an empty string for it.
        Do not include any extra text outside JSON.

        Text chunk:
        {chunk}
        """
        raw_text = call_gemini(prompt).strip()
        raw_text = re.sub(r"```json|```", "", raw_text).strip()

        try:
            data = json.loads(raw_text)
            for key in merged_data:
                if not merged_data[key] and key in data:
                    merged_data[key] = data[key]  # keep first non-empty value
        except json.JSONDecodeError:
            print("⚠ Could not parse JSON for a chunk.")

    return merged_data

def main():
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()

        for filepath in glob.glob(os.path.join(INPUT_DIR, "*.txt")):
            print(f"\n📂 Processing file: {filepath}")
            with open(filepath, "r", encoding="utf-8") as f:
                text = f.read()

            case_info = extract_case_info(text)
            case_info["File Name"] = os.path.basename(filepath)
            writer.writerow(case_info)

    print(f"\n✅ Extraction complete! Data saved in {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
