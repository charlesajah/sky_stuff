#!/usr/bin/python
###################################################################################
# Name : in_flight_sql_perf.py
# Purpose : Create confluence page (+ child pages) of database analysis
#     Called by jenkins job https://ppejenkins.bskyb.com/view/NFT%20DBA/view/NFT%20DBA%20-%20Analysis%20Jobs/job/NFT%20DBA%20-%20AWR%20Report%20Generation/
#     Based on https://stackoverflow.com/questions/33168060/how-can-i-create-a-new-page-to-confluence-with-python
# Parameters : 1 = p_test_description free text description field in jenkins job
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
parser = argparse.ArgumentParser()
parser.add_argument('p_test_description')
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
def add_labels(page_id, labels):
    # exp. https://example.com/wiki
    headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }
    try:
        response = requests.post(
            url + "{0}/label".format(page_id),
            auth = basic_auth,
            data = json.dumps([
                   {
                      "prefix": "global",
                      "name": labels
                   }
                             ] ),
            headers = headers)
        response.raise_for_status()

        result = response.json()

        page_url = url + '/pages/viewpage.action?pageId={0}'.format(page_id)
        return {'status': 'success', 'message': 'labels added', 'url': page_url}
    except requests.exceptions.HTTPError as err:
        return {'status': 'fail', 'message': "add label failed: {0}".format(err)}
    except:
        return {'status': 'fail', 'message': "Unexpected error: {0}".sys.exc_info()[0]} 
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
# main block
page_name = " ".join(args.p_test_description.split())  # strip out extra space characters (if any)
if page_name == '':
    page_name = '{:%Y%m%d%H%M%S}'.format(datetime.datetime.now())
elif pageExists(page_name):
    page_name = page_name + ' {:%Y%m%d%H%M%S}'.format(datetime.datetime.now())
page_id = createPage(page_name, 350887276, 'in_flight_sql_comparison.txt') # 350887276 is 'NFT - In-Flight Test SQL Comparison' page, under "NFT Central Analysis Reporting"
addlabelstatus =add_labels(page_id, 'nft_dba')
print(f"Confluence page created as https://confluence.bskyb.com/display/nonfuntst/{page_name.replace(' ', '+')} ")
###################################################################################