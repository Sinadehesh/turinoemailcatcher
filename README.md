# Turin Business Email Extractor 🇮🇹

A two-stage lead generation pipeline for extracting B2B email addresses from local businesses in Turin (Torino), Italy.

## Overview

This tool automates the process of finding and extracting email addresses from:
- **Shops** (retail stores, boutiques, specialty shops)
- **Restaurants & Cafes** (dining establishments, bars, pubs)
- **Service Businesses** (hairdressers, beauty salons, spas)
- **Other Local Amenities** (supermarkets, convenience stores, etc.)

## Architecture

### Stage 1: OpenStreetMap (Overpass API) Harvesting
- Queries the Overpass API for businesses within Turin's administrative boundary
- Filters for `shop=*` and `amenity=*` tags with valid `website` or `contact:website`
- Extracts business name, category, and website URL

### Stage 2: Asynchronous Email Extraction
- Uses `aiohttp` and `asyncio` for high-speed, non-blocking web crawling
- Checks homepage and common contact paths (`/contatti`, `/contact`, `/chi-siamo`, etc.)
- Applies regex patterns to extract valid email addresses
- Filters out image extensions and tracking pixels

### Stage 3: Data Cleaning & Export
- Deduplicates by domain and email address
- Filters out generic addresses (privacy@, webmaster@, admin@, abuse@, noreply@, etc.)
- **Keeps role-based business inboxes** (info@, contatti@, contact@) - essential for Italian SMBs
- Exports to CSV with headers: `Business_Name`, `Category`, `Website`, `Email`

## Installation

```bash
# Clone the repository
git clone https://github.com/Sinadehesh/turinoemailcatcher.git
cd turinoemailcatcher

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Run the pipeline
python turin_email_pipeline.py
```

### Output

The script generates a CSV file named `turin_businesses_YYYYMMDD_HHMMSS.csv` containing:

| Business_Name | Category | Website | Email |
|--------------|----------|---------|-------|
| Example Cafe | cafe | https://examplecafe.it | contact@examplecafe.it |
| Bella Boutique | clothes | https://bellaboutique.com | info@bellaboutique.com |

## Configuration

Edit the constants at the top of `turin_email_pipeline.py` to customize:

- `MAX_CONCURRENT_REQUESTS`: Number of parallel requests (default: 20)
- `TIMEOUT_SECONDS`: Request timeout in seconds (default: 15)
- `REQUEST_DELAY`: Delay between requests to same domain (default: 0.5s)
- `FILTER_EMAIL_PATTERNS`: Regex patterns for emails to exclude

## Compliance & Best Practices

✅ **Respects Overpass API limits** - Uses `[timeout:90]` in QL query  
✅ **Rotating User-Agents** - Prevents blocking with realistic headers  
✅ **Rate Limiting** - Built-in delays between requests  
✅ **Error Handling** - Graceful handling of dead servers and timeouts  
✅ **GDPR Consideration** - Filters out privacy@ and abuse@ addresses  
✅ **Italian SMB Optimized** - Keeps info@, contatti@, contact@ for local businesses  

## Technical Stack

- **Python 3.8+**
- **aiohttp** - Async HTTP client
- **BeautifulSoup4** - HTML parsing
- **pandas** - Data manipulation and CSV export
- **requests** - Synchronous HTTP (fallback)

## Expected Results

- **Target**: 1,000+ valid B2B email addresses
- **Success Rate**: ~40-60% of websites contain extractable emails
- **Runtime**: ~10-20 minutes for 1,000+ businesses (depending on concurrency)

## Troubleshooting

### Overpass API returns empty results
- The admin boundary query may need adjustment
- Try using relation ID instead of name lookup
- Check Overpass API status at https://overpass-api.de/

### Too many timeouts
- Reduce `MAX_CONCURRENT_REQUESTS` to 10
- Increase `TIMEOUT_SECONDS` to 30

### Low email extraction rate
- Expand `CONTACT_PATHS` list with additional Italian paths
- Some businesses use contact forms instead of email addresses

## License

MIT License - Feel free to use for your lead generation campaigns!

## Author

Built for local business outreach in Turin, Italy.

---

**Note**: This tool is for legitimate B2B outreach purposes only. Always comply with GDPR and local email marketing regulations.
