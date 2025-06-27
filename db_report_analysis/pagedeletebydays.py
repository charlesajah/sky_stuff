import requests
import json
import argparse
import datetime
import sys
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
parser = argparse.ArgumentParser()
parser.add_argument('p_days', type=int)
args = parser.parse_args()
# Config
URL = 'https://confluence.bskyb.com/rest/api/content/'
EMAIL = "your.email@example.com"
auth_token = 'nftauto'
basic_auth = HTTPBasicAuth('nft-upload', auth_token)
SPACE_KEY = 'nonfuntst'
ancestor_id = "333202389"  # The ID of the parent (top-level) page
noof_days = args.p_days
print(f"Noof_days: {args.p_days}")
page_age = (datetime.now() - timedelta(days=noof_days)).strftime('%Y-%m-%d')


# Endpoint for searching content
url = f"{URL}search"

# CQL: Confluence Query Language
cql = f"parent = {ancestor_id} AND type = page AND lastmodified <{page_age}"

params = {
    "cql": cql,
    "expand": "history",
    "limit": 100  # Increase if needed
}


# Send request
response = requests.get(
    url,
    auth=basic_auth,
    params=params
)

headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }

# Handle response
if response.status_code == 200:
    data = response.json()
    pages = data.get("results", [])
    for page in pages:
        title = page["title"]
        created_date = page["history"]["createdDate"]
        pageid = page["id"]
        print(f"Pages Deleted - Id: {pageid}, Page Title: {title}, Created: {created_date}")
        requests.delete("https://confluence.bskyb.com/rest/api/content/"+pageid+"", headers=headers, auth=basic_auth)
else:
    print("Failed to fetch data:", response.status_code, response.text)
