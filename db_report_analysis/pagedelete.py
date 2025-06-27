#!/usr/bin/python
###################################################################################
# Name : Pagedelete.py
# Purpose : Delete confluence page of daily database analysis pages.
#     Called by jenkins job https://ppejenkins.bskyb.com/view/NFT%20DBA/view/NFT%20DBA%20-%20Analysis%20Jobs/view/NFT%20DBA%20-%20Analysis%20Jobs%20(AWR%20+%20Confluence)/job/N01%20Delete%20Daily%20Auto%20Comparison%20Pages/
#     Based on https://stackoverflow.com/questions/33168060/how-can-i-create-a-new-page-to-confluence-with-python
# Parameters : 1 = P_PAGES - A generated list of Page titles to be Deleted.
# Change History :
#     21-NOV-2023 SM  initial version
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
parser = argparse.ArgumentParser()
parser.add_argument('P_PAGES')
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
    # Set the title and content of the page to create. Utf8 encoding is needed to deal with 201a low-9 quotation mark symbol in activity charts.
    with open(inputTextFile, 'r', encoding='utf8') as text_file:
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
##################################################################################
# main block
#headers = {
#        'Content-Type': 'application/json;charset=iso-8859-1',
#    }
#requests.delete("https://confluence.bskyb.com/rest/api/content/302423083", headers=headers, auth=basic_auth)
#deletePageByTitle("A test page to be deleted.")
page_name = " ".join(args.P_PAGES.split(","))
print(page_name)
n = len(sys.argv[1]) 
a = sys.argv[1][1:n-1] 
a = a.split(',') 
  
for i in a: 
 print(i)
 deletePageByTitle(i)
###################################################################################