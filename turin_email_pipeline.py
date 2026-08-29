#!/usr/bin/env python3
"""
Turin Business Email Extractor - Two-Stage Lead Generation Pipeline
Stage 1: Overpass API harvesting (OpenStreetMap)
Stage 2: Asynchronous email extraction from websites
Stage 3: Data cleaning and CSV export

Target: Shops, restaurants, salons, cafes in Turin (Torino), Italy
"""

import asyncio
import aiohttp
import re
import json
import csv
from typing import List, Dict, Set, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import time
from datetime import datetime
import random

# ============================================================================
# CONFIGURATION
# ============================================================================

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
TIMEOUT_SECONDS = 15
MAX_CONCURRENT_REQUESTS = 20
REQUEST_DELAY = 0.5  # seconds between requests to same domain

# Common contact page paths (Italian + English)
CONTACT_PATHS = [
    "",  # homepage
    "/contatti",
    "/contact",
    "/contatti-e-privacy",
    "/chi-siamo",
    "/about",
    "/about-us",
    "/informazioni",
    "/privacy",
    "/gdpr",
]

# Email regex pattern - matches standard email formats
EMAIL_REGEX = re.compile(
    r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
)

# Emails to filter out (generic/abuse addresses)
# NOTE: Kept role-based business inboxes (info@, contatti@, contact@) for Italian SMBs
FILTER_EMAIL_PATTERNS = [
    r'^privacy@',
    r'^webmaster@',
    r'^admin@',
    r'^abuse@',
    r'^noreply@',
    r'^no-reply@',
    r'^newsletter@',
    r'^marketing@',
    r'^support@',
    r'^help@',
    r'^sales@',
]

# Rotating User-Agent headers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ============================================================================
# STAGE 1: OVERPASS API HARVESTING
# ============================================================================

def build_overpass_query() -> str:
    """
    Build Overpass QL query targeting Turin administrative boundary.
    Filters for shops and amenities with website/contact:website tags.
    """
    query = """
    [out:json][timeout:90];
    
    // Get Turin administrative boundary (admin_level=8 for city)
    area["name"="Torino"]["admin_level"="8"]["boundary"="administrative"] -> .turin;
    
    // Collect all shops and amenities with websites
    (
      // Shops with websites
      node["shop"]["website"](area.turin);
      way["shop"]["website"](area.turin);
      relation["shop"]["website"](area.turin);
      
      // Shops with contact:website
      node["shop"]["contact:website"](area.turin);
      way["shop"]["contact:website"](area.turin);
      relation["shop"]["contact:website"](area.turin);
      
      // Amenities (restaurants, cafes, bars, etc.) with websites
      node["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["website"](area.turin);
      way["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["website"](area.turin);
      relation["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["website"](area.turin);
      
      // Amenities with contact:website
      node["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["contact:website"](area.turin);
      way["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["contact:website"](area.turin);
      relation["amenity"~"^(restaurant|cafe|bar|pub|fast_food|ice_cream|bakery|confectionery|hairdresser|beauty|clothes|shoes|jewelry|supermarket|convenience)"]["contact:website"](area.turin);
    );
    
    // Output with required tags
    out body;
    """
    return query


async def fetch_overpass_data(session: aiohttp.ClientSession) -> List[Dict]:
    """
    Fetch business data from Overpass API.
    Returns list of businesses with name, category, and website.
    """
    query = build_overpass_query()
    
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json"
    }
    
    try:
        async with session.post(
            OVERPASS_API_URL,
            data=query,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("elements", [])
            else:
                print(f"❌ Overpass API error: {response.status}")
                return []
    except Exception as e:
        print(f"❌ Overpass fetch failed: {e}")
        return []


def parse_overpass_results(elements: List[Dict]) -> List[Dict]:
    """
    Parse Overpass JSON response to extract business info.
    Returns list of {name, category, website} dicts.
    """
    businesses = []
    seen_urls: Set[str] = set()
    
    for elem in elements:
        tags = elem.get("tags", {})
        
        # Get website URL (prefer contact:website over website)
        website = tags.get("contact:website") or tags.get("website")
        if not website:
            continue
        
        # Normalize website URL
        if not website.startswith(("http://", "https://")):
            website = f"https://{website}"
        
        # Skip duplicates
        domain = urlparse(website).netloc
        if domain in seen_urls:
            continue
        seen_urls.add(domain)
        
        # Extract business name
        name = tags.get("name") or tags.get("brand") or "Unknown"
        
        # Extract category
        category = tags.get("shop") or tags.get("amenity") or "other"
        
        businesses.append({
            "name": name,
            "category": category,
            "website": website
        })
    
    print(f"✅ Found {len(businesses)} unique businesses with websites")
    return businesses


# ============================================================================
# STAGE 2: ASYNCHRONOUS EMAIL EXTRACTION
# ============================================================================

async def fetch_page(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str:
    """
    Fetch a single webpage with rate limiting.
    Returns HTML content or empty string on failure.
    """
    async with semaphore:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        }
        
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                allow_redirects=True
            ) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        
        return ""


def extract_emails_from_html(html: str, base_url: str) -> List[str]:
    """
    Extract email addresses from HTML content using regex.
    Filters out image extensions and common tracking pixels.
    """
    if not html:
        return []
    
    # Find all email matches
    emails = EMAIL_REGEX.findall(html)
    
    # Filter out invalid emails
    valid_emails = []
    for email in emails:
        # Skip if looks like image/file extension
        if email.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')):
            continue
        
        # Skip if contains tracking parameters
        if '?' in email or '&' in email:
            continue
        
        valid_emails.append(email.lower())
    
    return valid_emails


async def crawl_website(session: aiohttp.ClientSession, business: Dict, semaphore: asyncio.Semaphore) -> List[str]:
    """
    Crawl a business website to find email addresses.
    Checks homepage and common contact paths.
    """
    base_url = business["website"]
    domain = urlparse(base_url).netloc
    all_emails: List[str] = []
    
    # Try each contact path
    for path in CONTACT_PATHS:
        url = urljoin(base_url, path) if path else base_url
        
        html = await fetch_page(session, url, semaphore)
        if html:
            emails = extract_emails_from_html(html, base_url)
            all_emails.extend(emails)
        
        # Small delay to respect rate limits
        await asyncio.sleep(REQUEST_DELAY)
    
    # Deduplicate emails for this business
    unique_emails = list(set(all_emails))
    
    return unique_emails


# ============================================================================
# STAGE 3: DATA CLEANING & EXPORT
# ============================================================================

def filter_emails(emails: List[str]) -> List[str]:
    """
    Filter out generic/abuse email addresses.
    Returns list of valid B2B emails.
    NOTE: Keeps role-based inboxes (info@, contatti@, contact@) for Italian SMBs.
    """
    filtered = []
    
    for email in emails:
        # Check against filter patterns
        is_generic = any(re.match(pattern, email) for pattern in FILTER_EMAIL_PATTERNS)
        
        if not is_generic:
            filtered.append(email)
    
    return filtered


def deduplicate_and_clean(data: List[Dict]) -> List[Dict]:
    """
    Deduplicate by domain and email.
    Clean and validate all fields.
    """
    seen: Set[Tuple[str, str]] = set()
    cleaned = []
    
    for record in data:
        domain = urlparse(record["website"]).netloc
        email = record["email"]
        
        # Skip if same domain+email combo already exists
        key = (domain, email)
        if key in seen:
            continue
        
        seen.add(key)
        cleaned.append(record)
    
    return cleaned


def export_to_csv(data: List[Dict], filename: str = "turin_businesses.csv"):
    """
    Export cleaned data to CSV file.
    """
    if not data:
        print("⚠️  No data to export")
        return
    
    df = pd.DataFrame(data)
    df = df[["Business_Name", "Category", "Website", "Email"]]
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    
    print(f"✅ Exported {len(data)} records to {filename}")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

async def run_pipeline():
    """
    Execute the complete email extraction pipeline.
    """
    print("=" * 60)
    print("🚀 Turin Business Email Extractor Pipeline")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # -------------------------------------------------------------------------
    # STAGE 1: Overpass Harvesting
    # -------------------------------------------------------------------------
    print("📍 STAGE 1: Harvesting businesses from OpenStreetMap...")
    
    async with aiohttp.ClientSession() as session:
        overpass_elements = await fetch_overpass_data(session)
    
    businesses = parse_overpass_results(overpass_elements)
    
    if not businesses:
        print("❌ No businesses found. Exiting.")
        return
    
    print(f"   Found {len(businesses)} target businesses")
    print()
    
    # -------------------------------------------------------------------------
    # STAGE 2: Async Email Extraction
    # -------------------------------------------------------------------------
    print("📧 STAGE 2: Extracting emails from websites...")
    print(f"   Concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print()
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            crawl_website(session, business, semaphore)
            for business in businesses
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Combine results
    extracted_data = []
    for i, emails in enumerate(results):
        if isinstance(emails, Exception):
            print(f"   ⚠️  Error processing {businesses[i]['name']}: {emails}")
            continue
        
        if emails:
            # Filter emails
            filtered = filter_emails(emails)
            
            # Add each email as separate record
            for email in filtered:
                extracted_data.append({
                    "Business_Name": businesses[i]["name"],
                    "Category": businesses[i]["category"],
                    "Website": businesses[i]["website"],
                    "Email": email
                })
    
    print(f"   Extracted {len(extracted_data)} valid emails")
    print()
    
    # -------------------------------------------------------------------------
    # STAGE 3: Data Cleaning & Export
    # -------------------------------------------------------------------------
    print("🧹 STAGE 3: Cleaning and deduplicating data...")
    
    cleaned_data = deduplicate_and_clean(extracted_data)
    
    # Export to CSV
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
    """
    Entry point for the pipeline.
    """
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
