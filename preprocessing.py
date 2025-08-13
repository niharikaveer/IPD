# preprocessing_step1.py
import os
import re
import json
import hashlib
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# Attempt to import tiktoken for accurate token counts (optional).
try:
    import tiktoken
    tiktoken_encoder = tiktoken.get_encoding("cl100k_base")
except Exception:
    tiktoken_encoder = None

CSV_PATH = "extracted_cases_clean.csv"   # change if needed
SCRAPPED_TEXT_DIR = "scrappedText"       # used to attach file paths if present
OUTPUT_JSONL = "cases_chunks.jsonl"

# -------------------------
# Utilities
# -------------------------
def find_text_column(df):
    # heuristics to find the main cleaned text column
    candidates = [c for c in df.columns if re.search(r"(text|body|content|clean)", c, flags=re.I)]
    if candidates:
        return candidates[0]
    # fallback: longest average string length
    lengths = {c: df[c].astype(str).map(len).mean() for c in df.columns}
    return max(lengths, key=lengths.get)

def estimate_tokens(text):
    if tiktoken_encoder:
        return len(tiktoken_encoder.encode(text))
    # fallback: approximate tokens = words * 1.33
    words = len(text.split())
    return int(words * 1.33)

def normalize_date(date_val):
    # try common date formats; returns ISO string or None
    if pd.isna(date_val):
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(str(date_val).strip(), fmt).date().isoformat()
        except Exception:
            continue
    try:
        # pandas parse fallback
        parsed = pd.to_datetime(date_val, dayfirst=True, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date().isoformat()
    except Exception:
        return None

def hash_text(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

# Chunking function: uses sentence accumulation (robust if no tokenizer).
def chunk_text_by_tokens(text, chunk_size_tokens=700, overlap_tokens=100):
    # If tiktoken available, slice by token ids; otherwise accumulate sentences until approx size.
    if tiktoken_encoder:
        tok_ids = tiktoken_encoder.encode(text)
        chunks = []
        i = 0
        n = len(tok_ids)
        while i < n:
            chunk_ids = tok_ids[i: i + chunk_size_tokens]
            text_chunk = tiktoken_encoder.decode(chunk_ids)
            chunks.append(text_chunk.strip())
            i += chunk_size_tokens - overlap_tokens
        return chunks
    # fallback: split into sentences and accumulate
    sentences = re.split(r'(?<=[\.\?\!])\s+', text)
    chunks = []
    cur = []
    cur_words = 0
    est_words_per_token = 0.75  # inverse of 1.33
    target_words = int(chunk_size_tokens / est_words_per_token)
    overlap_words = int(overlap_tokens / est_words_per_token)
    for s in sentences:
        w = len(s.split())
        if cur_words + w > target_words and cur:
            chunks.append(" ".join(cur).strip())
            # start next chunk with overlap
            if overlap_words > 0:
                # take last overlap_words from cur
                last = " ".join(" ".join(cur).split()[-overlap_words:])
                cur = [last] if last else []
                cur_words = len(last.split()) if last else 0
            else:
                cur = []
                cur_words = 0
        cur.append(s)
        cur_words += w
    if cur:
        chunks.append(" ".join(cur).strip())
    return chunks

# -------------------------
# Main preprocessing
# -------------------------
print("Loading CSV:", CSV_PATH)
df = pd.read_csv(CSV_PATH, dtype=str).fillna("")

print("Columns found:", list(df.columns))

text_col = find_text_column(df)
print("Detected text column:", text_col)

# Basic checks
print("Total rows:", len(df))
# show the first 3 rows (some columns)
print("Sample rows:")
print(df.head(3).T)

# compute some metadata columns if present (safe access)
for col in ["Case Title", "File Name", "Case Number", "Court Name", "Date of Judgment"]:
    if col not in df.columns:
        df[col] = ""

# Normalize date column
print("Normalizing dates...")
df["date_normalized"] = df["Date of Judgment"].apply(normalize_date)

# token & word counts
print("Estimating token counts...")
df["word_count"] = df[text_col].astype(str).apply(lambda t: len(t.split()))
df["token_estimate"] = df[text_col].astype(str).apply(estimate_tokens)

# dedupe by case number or text hash
if "Case Number" in df.columns and df["Case Number"].str.strip().replace("", pd.NA).notna().any():
    before = len(df)
    df = df.drop_duplicates(subset=["Case Number"], keep="first")
    after = len(df)
    print(f"Dropped {before-after} duplicate rows by Case Number.")
else:
    # dedupe by text hash
    df["text_hash"] = df[text_col].astype(str).apply(hash_text)
    before = len(df)
    df = df.drop_duplicates(subset=["text_hash"], keep="first")
    after = len(df)
    print(f"Dropped {before-after} duplicate rows by text hash.")

# Prepare chunks and write JSONL
print("Chunking texts and writing to", OUTPUT_JSONL)
total_chunks = 0
with open(OUTPUT_JSONL, "w", encoding="utf-8") as fout:
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        file_name = row.get("File Name") or row.get("file_name") or f"row_{idx}"
        case_title = row.get("Case Title") or row.get("Case_Title") or row.get("case_title") or ""
        court = row.get("Court Name") or ""
        case_number = row.get("Case Number") or ""
        date_j = row.get("date_normalized") or ""
        judges = row.get("Judges") or ""
        petitioner = row.get("Petitioner(s)") or row.get("Petitioner") or ""
        respondent = row.get("Respondent(s)") or row.get("Respondent") or ""
        legal_issues = row.get("Legal Issues") or ""
        outcome = row.get("Outcome") or ""
        citations = row.get("Citations") or ""
        full_text = row[text_col] if text_col in row else str(row)

        # find scrapped file path if exists
        local_path = ""
        candidate_path = os.path.join(SCRAPPED_TEXT_DIR, file_name)
        if os.path.exists(candidate_path):
            local_path = os.path.abspath(candidate_path)
        else:
            # try file name with .txt
            p2 = candidate_path if candidate_path.endswith(".txt") else candidate_path + ".txt"
            if os.path.exists(p2):
                local_path = os.path.abspath(p2)

        chunks = chunk_text_by_tokens(full_text, chunk_size_tokens=700, overlap_tokens=120)
        for i, chunk in enumerate(chunks):
            doc = {
                "id": f"{file_name}__chunk_{i}",
                "text": chunk,
                "metadata": {
                    "file_name": file_name,
                    "case_title": case_title,
                    "court": court,
                    "case_number": case_number,
                    "date": date_j,
                    "judges": judges,
                    "petitioner": petitioner,
                    "respondent": respondent,
                    "legal_issues": legal_issues,
                    "outcome": outcome,
                    "citations": citations,
                    "local_path": local_path  # path to original txt if available
                }
            }
            fout.write(json.dumps(doc, ensure_ascii=False) + "\n")
            total_chunks += 1

print("Done. Total chunks written:", total_chunks)
print("Output file:", OUTPUT_JSONL)
