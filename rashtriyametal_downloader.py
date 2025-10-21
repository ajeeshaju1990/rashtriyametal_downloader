# rashtriyametal_downloader.py
# Purpose:
# - Scrape latest Rashtriya Metal circular PDF
# - Save into data/RashtriyaMetal/PDFs/
# - Extract table from the PDF and append into data/RashtriyaMetal/RMIL_Table.xlsx
# - Header written once; subsequent runs only append new circular rows (de-duplicated by pdf_url + row content)
#
# Deps: requests, beautifulsoup4, pdfplumber, pandas, openpyxl, (optional) camelot-py[cv]

import os
import re
import io
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pdfplumber

# Try Camelot if present (better tables). If not, we'll gracefully fall back.
try:
    import camelot
    HAS_CAMELOT = True
except Exception:
    HAS_CAMELOT = False

# -------------------------
# CONFIG (repo-relative)
# -------------------------
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(REPO_ROOT, "data", "RashtriyaMetal")
PDF_DIR = os.path.join(DATA_DIR, "PDFs")
EXCEL_TABLE = os.path.join(DATA_DIR, "RMIL_Table.xlsx")
EXCEL_URL_LOG = os.path.join(DATA_DIR, "RMIL_Price_Log.xlsx")  # keeps only the last URL; also useful for audits

CIRCULARS_PAGE = "https://rashtriyametal.com/price-circulars/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36"


def ensure_dirs():
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)


def fetch_latest_pdf_url():
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(CIRCULARS_PAGE, headers=headers, timeout=45)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            links.append(urljoin(CIRCULARS_PAGE, href))

    if not links:
        # Look for URLs embedded in onclick/data-href
        for tag in soup.find_all():
            for attr in ("data-href", "onclick"):
                val = tag.get(attr) or ""
                m = re.search(r'(https?://[^\s"\'<>]+\.pdf)', val, flags=re.I)
                if m:
                    links.append(m.group(1))

    if not links:
        raise RuntimeError("No PDF links found on the circulars page.")

    # Prefer the one with the latest date in filename (e.g., rmil16102025...)
    dated = []
    for u in links:
        fname = os.path.basename(urlparse(u).path)
        m = (re.search(r'(\d{2})(\d{2})(\d{4})', fname) or
             re.search(r'(20\d{2})(\d{2})(\d{2})', fname))
        if m:
            try:
                if len(m.groups()) == 3 and len(m.group(1)) == 4:
                    d = datetime.strptime("".join(m.groups()), "%Y%m%d")
                else:
                    d = datetime.strptime("".join(m.groups()), "%d%m%Y")
                dated.append((d, u))
            except Exception:
                pass
    if dated:
        dated.sort(key=lambda x: x[0], reverse=True)
        return dated[0][1]

    # Fallback: first link on the page
    return links[0]


def read_last_logged_url():
    if not os.path.exists(EXCEL_URL_LOG):
        return None
    try:
        df = pd.read_excel(EXCEL_URL_LOG)
        if "pdf_url" in df.columns and not df["pdf_url"].empty:
            return str(df["pdf_url"].iloc[-1])
    except Exception:
        pass
    return None


def write_url_log(pdf_url, local_pdf, circular_date):
    cols = ["timestamp", "circular_date", "pdf_url", "local_pdf"]
    new_row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "circular_date": circular_date or "",
        "pdf_url": pdf_url,
        "local_pdf": local_pdf,
    }
    if os.path.exists(EXCEL_URL_LOG):
        df = pd.read_excel(EXCEL_URL_LOG)
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = pd.concat([df, pd.DataFrame([new_row], columns=cols)], ignore_index=True)
    else:
        df = pd.DataFrame([new_row], columns=cols)
    df.to_excel(EXCEL_URL_LOG, index=False)


def download_pdf(pdf_url):
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(pdf_url, headers=headers, timeout=90)
    r.raise_for_status()
    fname = os.path.basename(urlparse(pdf_url).path)
    fname = re.sub(r'[^A-Za-z0-9._-]+', '_', fname)
    local_path = os.path.join(PDF_DIR, fname)
    with open(local_path, "wb") as f:
        f.write(r.content)
    return local_path


def extract_date_from_text(text, fallback_from_name=None):
    # Try several date patterns inside the PDF text
    date_candidates = []
    for pat, fmt in [
        (r'(\d{1,2})[/-](\d{1,2})[/-](20\d{2})', "%d-%m-%Y"),
        (r'(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})', "%d %b %Y"),
        (r'([A-Za-z]{3,9})\s+(\d{1,2}),\s*(20\d{2})', "%b %d, %Y"),
    ]:
        m = re.search(pat, text, flags=re.I)
        if m:
            try:
                if fmt == "%d-%m-%Y":
                    d = datetime.strptime(f"{m.group(1)}-{m.group(2)}-{m.group(3)}", fmt)
                elif fmt == "%d %b %Y":
                    d = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt)
                else:
                    d = datetime.strptime(f"{m.group(1)} {m.group(2)}, {m.group(3)}", fmt)
                date_candidates.append(d)
            except Exception:
                pass
    if date_candidates:
        return max(date_candidates).strftime("%Y-%m-%d")

    # fallback: from filename like ddmmyyyy
    if fallback_from_name:
        m = re.search(r'(\d{2})(\d{2})(20\d{2})', fallback_from_name)
        if m:
            try:
                d = datetime.strptime("".join(m.groups()), "%d%m%Y")
                return d.strftime("%Y-%m-%d")
            except Exception:
                pass
    return ""


def read_pdf_text(local_pdf_path):
    with open(local_pdf_path, "rb") as f:
        raw = f.read()
    text_all = []
    with pdfplumber.open(io.BytesIO(raw)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text_all.append(t)
    return "\n".join(text_all)


def try_extract_tables_with_camelot(local_pdf_path):
    """
    Returns a DataFrame by concatenating all detected tables (first few pages),
    or None if Camelot isn't available or found nothing.
    """
    if not HAS_CAMELOT:
        return None
    try:
        # lattice works on ruled tables, stream on whitespace-separated
        tables_lattice = camelot.read_pdf(local_pdf_path, pages='1-end', flavor='lattice')
        dfs = [t.df for t in tables_lattice] if tables_lattice else []
        if not dfs:
            tables_stream = camelot.read_pdf(local_pdf_path, pages='1-end', flavor='stream')
            dfs = [t.df for t in tables_stream] if tables_stream else []
        if not dfs:
            return None

        # Normalize headers = first row
        norm = []
        for df in dfs:
            df = df.copy()
            df.columns = df.iloc[0].astype(str).str.strip()
            df = df.iloc[1:].reset_index(drop=True)
            norm.append(df)
        out = pd.concat(norm, ignore_index=True)
        # Drop completely empty columns/rows
        out = out.dropna(how='all', axis=1).dropna(how='all', axis=0)
        return out
    except Exception:
        return None


def fallback_extract_table_with_pdfplumber(local_pdf_path):
    """
    Very simple fallback: split lines into 'columns' on multiple spaces/tabs.
    This is heuristic — good enough to ensure something is captured.
    """
    rows = []
    with pdfplumber.open(local_pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                # split on 2+ spaces or tabs
                parts = re.split(r'\s{2,}|\t+', line.strip())
                if len(parts) > 1:
                    rows.append(parts)

    if not rows:
        return None

    # Infer header from the longest row near the top
    header = max(rows[:10], key=len) if rows else []
    df = pd.DataFrame(rows)
    df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
    # Try to promote header row if shapes match
    for r in rows[:10]:
        if len(r) == df.shape[1]:
            header = [h.strip() for h in r]
            df.columns = header
            # drop the first matching header row occurrence
            first_idx = df.index[(df.iloc[:, 0] == r[0])].tolist()
            if first_idx:
                df = df.drop(index=first_idx[0])
            break

    df = df.reset_index(drop=True)
    # Clean empty columns
    df = df.dropna(how='all', axis=1)
    return df


def append_table_to_excel(df_new, pdf_url, circular_date):
    """
    Write once (headers) and append on new circulars.
    We also store 'pdf_url' and 'circular_date' columns for traceability.
    De-duplicate on (pdf_url + full row string).
    """
    if df_new is None or df_new.empty:
        print("[WARN] No table detected to append.")
        return

    # Enrich with metadata
    df_new = df_new.copy()
    df_new.insert(0, "circular_date", circular_date or "")
    df_new.insert(1, "pdf_url", pdf_url)

    # Dedup key column (string of all cells)
    df_new["_row_key"] = df_new.astype(str).agg("|".join, axis=1)

    if os.path.exists(EXCEL_TABLE):
        df_old = pd.read_excel(EXCEL_TABLE)
        if "_row_key" not in df_old.columns:
            df_old["_row_key"] = df_old.astype(str).agg("|".join, axis=1)
        combo = pd.concat([df_old, df_new], ignore_index=True)
        combo = combo.drop_duplicates(subset=["pdf_url", "_row_key"])
        # Keep columns order: put metadata first
        meta_cols = [c for c in ["circular_date", "pdf_url"] if c in combo.columns]
        other_cols = [c for c in combo.columns if c not in meta_cols + ["_row_key"]]
        combo = combo[meta_cols + other_cols]
        combo.to_excel(EXCEL_TABLE, index=False)
    else:
        # First time: just save with headers
        # Keep the metadata columns at front
        meta_cols = ["circular_date", "pdf_url"]
        other_cols = [c for c in df_new.columns if c not in meta_cols + ["_row_key"]]
        out = df_new[meta_cols + other_cols].copy()
        out.to_excel(EXCEL_TABLE, index=False)

    print(f"[SUCCESS] Table appended into: {EXCEL_TABLE}")


def main():
    ensure_dirs()

    latest_url = fetch_latest_pdf_url()
    print(f"[INFO] Latest PDF URL: {latest_url}")

    last_url = read_last_logged_url()
    if last_url and last_url.strip() == latest_url.strip():
        print("[INFO] Same URL as last run. Skipping download + parse.")
        sys.exit(0)

    # Download PDF
    local_pdf = download_pdf(latest_url)
    print(f"[INFO] Downloaded to: {local_pdf}")

    # Extract date for tagging
    text = read_pdf_text(local_pdf)
    circular_date = extract_date_from_text(text, os.path.basename(local_pdf))
    print(f"[INFO] Circular date detected: {circular_date or '(not found)'}")

    # Extract table(s)
    df = None
    if HAS_CAMELOT:
        df = try_extract_tables_with_camelot(local_pdf)
        if df is not None:
            print("[INFO] Table extracted via Camelot.")
    if df is None:
        df = fallback_extract_table_with_pdfplumber(local_pdf)
        if df is not None:
            print("[INFO] Table extracted via pdfplumber fallback.")

    # Append to Excel (header only once; append next times)
    append_table_to_excel(df, latest_url, circular_date)

    # Write URL log (to prevent re-processing same circular)
    write_url_log(latest_url, local_pdf, circular_date)
    print(f"[SUCCESS] URL logged: {EXCEL_URL_LOG}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
