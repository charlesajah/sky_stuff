#!/usr/bin/python
###################################################################################
# Name : nft_space_usage_v2.py
# Purpose : Create confluence page for te Space Usage across the Oracle databases
#     
#     
###################################################################################
# initialize
import requests
import json
import argparse
import datetime
import sys
from requests.auth import HTTPBasicAuth
# Global variables: set auth token and get the basic auth code
auth_token = 'nftauto'
basic_auth = HTTPBasicAuth('nft-upload', auth_token)
space_key = 'nonfuntst'  # 'UK IS Platform Performance Engineering'
url = 'https://confluence.bskyb.com/rest/api/content/'
# Parameter to define page to be created
parser = argparse.ArgumentParser()
parser.add_argument('p_env')
args = parser.parse_args()
###################################################################################
def pageExists(page_title):   # checks if confluence page already exists
    # Request Headers
    headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }
    # Request body
    data = {
        'type': 'page',
        'title': page_title,
        'space': {'key':space_key}
    }
    try:
        r = requests.get(url=url, params=data, headers=headers, auth=basic_auth)
        # Consider any status other than 2xx an error
        if not r.status_code // 100 == 2:
            print("Error: Unexpected response {}".format(r))
            sys.exit()
        else:
            if 'id' in r.text and r.json()['results'][0]['id'].isdigit():
                return True
            else:
                return False
    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        print("Error: {}".format(e))
        sys.exit()
###################################################################################
def createPage(page_title, parent_page_id, inputTextFile):   # creates a confluence page, returns the new page ID.
    # Set the title and content of the page to create
    with open(inputTextFile, 'r') as text_file:
        page_html = text_file.read()
    # Request Headers
    headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }
    # Request body
    data = {
        'type': 'page',
        'title': page_title,
        'ancestors': [{'id':parent_page_id}],
        'space': {'key':space_key},
        'body': {
            'storage':{
            'value': page_html,
                'representation':'wiki',
            }
        }
    }
    try:
        r = requests.post(url=url, data=json.dumps(data), headers=headers, auth=basic_auth)
        # Consider any status other than 2xx an error
        if not r.status_code // 100 == 2:
            print("Error: Unexpected response {}".format(r))
            print(r.text)
            return('Error')
        else:
            return r.json()['id']
    except requests.exceptions.RequestException as e:
        # A serious problem happened, like an SSLError or InvalidURL
        print("Error: {}".format(e))
        return('Error')
###################################################################################
def deletePageByTitle(title):
    headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }
    checkPageExistsData = requests.get("https://confluence.bskyb.com/rest/api/content?title=" + title + "&expand=history", headers=headers, auth=basic_auth)
    requestJson = checkPageExistsData.json()
    pageId = ''
    if requestJson["results"] != None:
     for results in requestJson["results"]:
      pageId = (results["id"])
      requests.delete("https://confluence.bskyb.com/rest/api/content/"+pageId+"", headers=headers, auth=basic_auth)
      print('Page deleted')
    else:
      print('Page does not exist')
###################################################################################
# main block

prefix = " ".join(args.p_env.split()) # strip out extra space characters (if any)

page_name1 = prefix + " - Database Growth ad-hoc"
page_name2 = prefix + " - Schema Growth ad-hoc"
page_name3 = prefix + " - Tablespace Growth ad-hoc"

if prefix == '':
    prefix = 'NONE'
elif pageExists(page_name1): 
    deletePageByTitle(page_name1)
    deletePageByTitle(page_name2)
    deletePageByTitle(page_name3)
        
page_id = createPage(page_name1, 306333980, 'space.txt')  # 306333980 is 'Ad-Hoc Space Reporting' 
print("Database Growth Page created under https://confluence.bskyb.com/display/nonfuntst/+{page_name1.replace(' ', '+')} ")
page_id = createPage(page_name2, 306333980, 'schema_space.txt')  # 306333980 is 'Ad-Hoc Space Reporting' 
print("Schema Growth Page created under https://confluence.bskyb.com/display/nonfuntst/+{page_name2.replace(' ', '+')} ")
page_id = createPage(page_name3, 306333980, 'ts_space.txt')  # 306333980 is 'Ad-Hoc Space Reporting' 
print("Tablespace Growth Page created under https://confluence.bskyb.com/display/nonfuntst/+{page_name3.replace(' ', '+')} ")


###################################################################################
