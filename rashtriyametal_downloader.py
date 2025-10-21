# rashtriyametal_downloader.py
# Purpose: Find the latest Rashtriya Metal price circular PDF, download if new, extract key numbers, append to Excel.
# Requires: requests, beautifulsoup4, pdfplumber, openpyxl, pandas

import os
import re
import io
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd

# =========================
# CONFIG — EDIT IF NEEDED
# =========================
CIRCULARS_PAGE = "https://rashtriyametal.com/price-circulars/"
DEFAULT_PDF_HINT = "pdf"  # class/button hint on the page; we will fallback to all .pdf links
DOWNLOAD_DIR = r"D:\OneDrive - V-Guard Industries Limited\Ajeesh_Selenium_Automation\RashtriyaMetal\PDFs"
EXCEL_LOG = r"D:\OneDrive - V-Guard Industries Limited\Ajeesh_Selenium_Automation\RashtriyaMetal\RMIL_Price_Log.xlsx"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36"

# Optional: pin a sample PDF URL (useful if the page breaks). Comment out normally.
# SAMPLE_PDF_URL = "https://rashtriyametal.com/wp-content/uploads/2025/10/rmil16102025-pdff.pdf"
SAMPLE_PDF_URL = None

# =========================
# HELPERS
# =========================
def ensure_dirs():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    parent = os.path.dirname(EXCEL_LOG)
    if parent:
        os.makedirs(parent, exist_ok=True)

def fetch_latest_pdf_url():
    """
    Scrape the circulars page and return the most recent PDF link (based on
    filename date heuristics first; fallback to page order).
    """
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(CIRCULARS_PAGE, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Collect every absolute PDF link on the page
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            links.append(urljoin(CIRCULARS_PAGE, href))

    # Some pages use buttons that trigger the same link:
    # <button class="price-circular-button">PDF</button> next to an <a>
    # If nothing found, try to look for data attributes or JS blobs
    if not links:
        # last resort: scan all text for 'href="...pdf"'
        for tag in soup.find_all():
            data = tag.get("data-href") or tag.get("onclick") or ""
            m = re.search(r'(https?://[^\s"\'<>]+\.pdf)', data, flags=re.I)
            if m:
                links.append(m.group(1))

    if not links and SAMPLE_PDF_URL:
        return SAMPLE_PDF_URL

    if not links:
        raise RuntimeError("No PDF links found on the circulars page.")

    # Prefer link with a date embedded (e.g., rmil16102025-pdff.pdf)
    dated = []
    for u in links:
        fname = os.path.basename(urlparse(u).path)
        # Try DDMMYYYY or DDMMyy or YYYYMMDD patterns
        m = (re.search(r'(\d{2})(\d{2})(\d{4})', fname) or
             re.search(r'(\d{2})(\d{2})(\d{2})(?!\d)', fname) or
             re.search(r'(20\d{2})(\d{2})(\d{2})', fname))
        if m:
            try:
                if len(m.groups()) == 3 and len(m.group(3)) == 4:
                    # dd mm yyyy
                    d = datetime.strptime("".join(m.groups()), "%d%m%Y")
                elif len(m.groups()) == 3 and len(m.group(1)) == 4:
                    # yyyy mm dd
                    d = datetime.strptime("".join(m.groups()), "%Y%m%d")
                else:
                    # dd mm yy -> assume 20yy
                    dd, mm, yy = m.group(1), m.group(2), m.group(3)
                    d = datetime.strptime(dd + mm + ("20" + yy), "%d%m%Y")
                dated.append((d, u))
            except Exception:
                pass

    if dated:
        dated.sort(key=lambda x: x[0], reverse=True)
        return dated[0][1]

    # Fallback: first link on the page (often latest)
    return links[0]

def download_pdf(pdf_url):
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(pdf_url, headers=headers, timeout=60)
    resp.raise_for_status()
    fname = os.path.basename(urlparse(pdf_url).path)
    # Ensure a clean filename
    fname = re.sub(r'[^A-Za-z0-9._-]+', '_', fname)
    local_path = os.path.join(DOWNLOAD_DIR, fname)
    with open(local_path, "wb") as f:
        f.write(resp.content)
    return local_path

def pdf_text_bytes(pdf_bytes):
    text = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            text.append(t)
    return "\n".join(text)

def extract_fields_from_pdf(local_pdf_path):
    """
    Heuristic text parsing. Adjust patterns to match RMIL circular layout.
    Returns a dict ready to append to Excel.
    """
    with open(local_pdf_path, "rb") as f:
        raw = f.read()

    text = pdf_text_bytes(raw)

    # --- Extract date ---
    # Look for formats like 16/10/2025, 16-10-2025, 16 Oct 2025, October 16, 2025, etc.
    date_candidates = []
    for pat, fmt in [
        (r'(\d{1,2})[/-](\d{1,2})[/-](20\d{2})', "%d-%m-%Y"),
        (r'(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})', "%d %b %Y"),  # 16 Oct 2025 or 16 October 2025
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
        circular_date = max(date_candidates)
    else:
        # fallback: try picking from filename ddmmyyyy
        basename = os.path.basename(local_pdf_path)
        m = re.search(r'(\d{2})(\d{2})(20\d{2})', basename)
        if m:
            circular_date = datetime.strptime("".join(m.groups()), "%d%m%Y")
        else:
            circular_date = None

    # --- Extract price numbers ---
    # Tweak these based on RMIL content (examples below):
    # Rs/kg:  "Rs 248.50/kg", "₹ 248/kg", "INR 250 per Kg"
    # Rs/MT : "Rs 245000/MT", "₹ 245,000 PMT", "INR 245000 per MT"
    price_rs_per_kg = None
    kg_match = re.search(r'(?:Rs\.?|₹|INR)\s*([\d,]+(?:\.\d+)?)\s*(?:per\s*)?(?:kg|kilogram)', text, flags=re.I)
    if kg_match:
        price_rs_per_kg = float(kg_match.group(1).replace(",", ""))

    price_rs_per_mt = None
    mt_match = re.search(r'(?:Rs\.?|₹|INR)\s*([\d,]+(?:\.\d+)?)\s*(?:per\s*)?(?:MT|PMT|metric\s*ton)', text, flags=re.I)
    if mt_match:
        price_rs_per_mt = float(mt_match.group(1).replace(",", ""))

    # If only MT found, convert to kg
    if price_rs_per_kg is None and price_rs_per_mt is not None:
        price_rs_per_kg = round(price_rs_per_mt / 1000.0, 2)

    # Try to capture product/grade keywords (optional)
    # e.g., "Aluminium", "Copper", "Tin", "Scrap", etc.
    product = None
    for kw in ["Aluminium", "Aluminum", "Copper", "Tin", "Scrap", "Ingot", "Rod", "Wire", "Sheet"]:
        if re.search(rf'\b{kw}\b', text, flags=re.I):
            product = kw
            break

    return {
        "circular_date": circular_date.strftime("%Y-%m-%d") if circular_date else "",
        "rs_per_kg": price_rs_per_kg if price_rs_per_kg is not None else "",
        "rs_per_mt": price_rs_per_mt if price_rs_per_mt is not None else "",
        "product_hint": product if product else "",
        "raw_pdf_text_sample": (text[:500] + "...") if len(text) > 500 else text
    }

def read_last_logged_url():
    if not os.path.exists(EXCEL_LOG):
        return None
    try:
        df = pd.read_excel(EXCEL_LOG)
        if "pdf_url" in df.columns and not df["pdf_url"].empty:
            return str(df["pdf_url"].iloc[-1])
    except Exception:
        pass
    return None

def append_to_excel(row_dict, pdf_url, local_path):
    cols = ["timestamp", "circular_date", "pdf_url", "local_pdf", "product_hint", "rs_per_kg", "rs_per_mt"]
    new_row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "circular_date": row_dict.get("circular_date", ""),
        "pdf_url": pdf_url,
        "local_pdf": local_path,
        "product_hint": row_dict.get("product_hint", ""),
        "rs_per_kg": row_dict.get("rs_per_kg", ""),
        "rs_per_mt": row_dict.get("rs_per_mt", "")
    }

    if os.path.exists(EXCEL_LOG):
        df = pd.read_excel(EXCEL_LOG)
        # Ensure columns exist
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = pd.concat([df, pd.DataFrame([new_row], columns=cols)], ignore_index=True)
    else:
        df = pd.DataFrame([new_row], columns=cols)

    df.to_excel(EXCEL_LOG, index=False)

def main():
    ensure_dirs()

    latest_url = fetch_latest_pdf_url()
    print(f"[INFO] Latest PDF URL detected: {latest_url}")

    last_url = read_last_logged_url()
    if last_url and last_url.strip() == latest_url.strip():
        print("[INFO] Skipping download — latest URL matches last logged URL.")
        sys.exit(0)

    # Download
    local_pdf = download_pdf(latest_url)
    print(f"[INFO] Downloaded to: {local_pdf}")

    # Parse
    payload = extract_fields_from_pdf(local_pdf)
    print(f"[INFO] Parsed fields: {payload}")

    # Append to Excel
    append_to_excel(payload, latest_url, local_pdf)
    print(f"[SUCCESS] Logged to: {EXCEL_LOG}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
