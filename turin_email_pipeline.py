#!/usr/bin/env python3
"""Turin Business Email Extractor Pipeline - Fixed Overpass GET request"""

import asyncio
import aiohttp
import re
import json
from typing import List, Dict, Set
from urllib.parse import urljoin, urlparse, quote
import pandas as pd
from datetime import datetime
import random

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
TIMEOUT_SECONDS = 15
MAX_CONCURRENT_REQUESTS = 20
REQUEST_DELAY = 0.5

CONTACT_PATHS = ["", "/contatti", "/contact", "/chi-siamo", "/about", "/privacy"]

EMAIL_REGEX = re.compile(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b')

FILTER_EMAIL_PATTERNS = [r'^privacy@', r'^webmaster@', r'^admin@', r'^abuse@', r'^noreply@', r'^no-reply@', r'^newsletter@', r'^marketing@', r'^support@', r'^help@', r'^sales@']

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

def build_overpass_query() -> str:
    # Simplified query - test with just one category first
    query = """[out:json][timeout:25];(node["shop"="clothes"]["website"](45.03,7.57,45.12,7.72););out body;"""
    return query

async def fetch_overpass_data(session: aiohttp.ClientSession) -> List[Dict]:
    query = build_overpass_query()
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json"
    }
    # Use GET request with query parameter
    url = f"{OVERPASS_API_URL}?data={quote(query)}"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as response:
            response_text = await response.text()
            if response.status == 200:
                data = json.loads(response_text)
                return data.get("elements", [])
            else:
                print(f"❌ Overpass API error: {response.status}")
                print(f"Response: {response_text[:500]}")
                return []
    except Exception as e:
        print(f"❌ Overpass fetch failed: {e}")
        return []

def parse_overpass_results(elements: List[Dict]) -> List[Dict]:
    businesses = []
    seen_urls: Set[str] = set()
    for elem in elements:
        tags = elem.get("tags", {})
        website = tags.get("contact:website") or tags.get("website")
        if not website:
            continue
        if not website.startswith(("http://", "https://")):
            website = f"https://{website}"
        domain = urlparse(website).netloc
        if domain in seen_urls:
            continue
        seen_urls.add(domain)
        name = tags.get("name") or tags.get("brand") or "Unknown"
        category = tags.get("shop") or tags.get("amenity") or "other"
        businesses.append({"name": name, "category": category, "website": website})
    print(f"✅ Found {len(businesses)} unique businesses with websites")
    return businesses

async def fetch_page(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str:
    async with semaphore:
        headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "it-IT,it;q=0.9,en;q=0.8"}
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS), allow_redirects=True) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        return ""

def extract_emails_from_html(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    emails = EMAIL_REGEX.findall(html)
    valid_emails = []
    for email in emails:
        if email.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')):
            continue
        if '?' in email or '&' in email:
            continue
        valid_emails.append(email.lower())
    return valid_emails

async def crawl_website(session: aiohttp.ClientSession, business: Dict, semaphore: asyncio.Semaphore) -> List[str]:
    base_url = business["website"]
    all_emails: List[str] = []
    for path in CONTACT_PATHS:
        url = urljoin(base_url, path) if path else base_url
        html = await fetch_page(session, url, semaphore)
        if html:
            emails = extract_emails_from_html(html, base_url)
            all_emails.extend(emails)
        await asyncio.sleep(REQUEST_DELAY)
    return list(set(all_emails))

def filter_emails(emails: List[str]) -> List[str]:
    filtered = []
    for email in emails:
        is_generic = any(re.match(pattern, email) for pattern in FILTER_EMAIL_PATTERNS)
        if not is_generic:
            filtered.append(email)
    return filtered

def deduplicate_and_clean(data: List[Dict]) -> List[Dict]:
    seen: Set[tuple] = set()
    cleaned = []
    for record in data:
        domain = urlparse(record["website"]).netloc
        email = record["email"]
        key = (domain, email)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(record)
    return cleaned

def export_to_csv(data: List[Dict], filename: str = "turin_businesses.csv"):
    if not data:
        print("⚠️  No data to export")
        return
    df = pd.DataFrame(data)
    df = df[["Business_Name", "Category", "Website", "Email"]]
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"✅ Exported {len(data)} records to {filename}")

async def run_pipeline():
    print("=" * 60)
    print("🚀 Turin Business Email Extractor Pipeline")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("📍 STAGE 1: Harvesting businesses from OpenStreetMap...")
    async with aiohttp.ClientSession() as session:
        overpass_elements = await fetch_overpass_data(session)
    businesses = parse_overpass_results(overpass_elements)
    if not businesses:
        print("❌ No businesses found. Exiting.")
        return
    print(f"   Found {len(businesses)} target businesses")
    print()
    print("📧 STAGE 2: Extracting emails from websites...")
    print(f"   Concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [crawl_website(session, business, semaphore) for business in businesses]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    extracted_data = []
    for i, emails in enumerate(results):
        if isinstance(emails, Exception):
            print(f"   ⚠️  Error processing {businesses[i]['name']}: {emails}")
            continue
        if emails:
            filtered = filter_emails(emails)
            for email in filtered:
                extracted_data.append({"Business_Name": businesses[i]["name"], "Category": businesses[i]["category"], "Website": businesses[i]["website"], "Email": email})
    print(f"   Extracted {len(extracted_data)} valid emails")
    print()
    print("🧹 STAGE 3: Cleaning and deduplicating data...")
    cleaned_data = deduplicate_and_clean(extracted_data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"turin_businesses_{timestamp}.csv"
    export_to_csv(cleaned_data, output_file)
    print()
    print("=" * 60)
    print("✅ Pipeline Complete!")
    print(f"📊 Final count: {len(cleaned_data)} unique business emails")
    print(f"💾 Output file: {output_file}")
    print("=" * 60)

def main():
    asyncio.run(run_pipeline())

if __name__ == "__main__":
    main()
