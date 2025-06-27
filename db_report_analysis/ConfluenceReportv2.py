#!/usr/bin/python
###################################################################################
# Name : ConfluenceReport.py
# Purpose : Create confluence page (+ child pages) of database analysis
# Parameters : 1 = p_test_description free text description field in jenkins job
# Change History :
#     18/11/2023 : Duplicated to split the Confluence Creation Pages as stand-alone
#     30/08/2024 : The pages were moved to a new location --> 333202308 is 'Full Analysis Reports' page, that comes under "NFT Central Analysis Reporting"
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
parser.add_argument('p_label')
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
# Add a label to an existing page
#
def add_labels(page_id, labels):
    # exp. https://example.com/wiki
    headers = {
        'Content-Type': 'application/json;charset=iso-8859-1',
    }
    print(f"Into add_labels - "+page_id)
    print(f"Basic URL - "+url)
    print(f"URL for labels - "+url + "{0}/label".format(page_id))
    print(f"The label passed in "+labels)
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
#######################################################################################
#
# main block
labeltouse = " ".join(args.p_label.split())
page_name = " ".join(args.p_test_description.split())  # strip out extra space characters (if any)
if page_name == '':
    page_name = '{:%Y%m%d%H%M%S}'.format(datetime.datetime.now())
#elif pageExists("DB Analysis Summary - " + page_name) or pageExists("1. Database Analysis " + page_name) or pageExists("5. Load Comparison " + page_name) or pageExists("6. All SQL Comparison " + page_name) or pageExists("4. Sql Analysis " + page_name) or pageExists("3. Database Activity " + page_name) or pageExists("Delete " + page_name):
#    page_name = page_name + ' {:%Y%m%d%H%M%S}'.format(datetime.datetime.now())
page_id = createPage("DB Analysis Summary - " + page_name, 333202308, 'CentralDBSummary.txt')  # 333202308 is 'Full Analysis Reports' page, that comes under "NFT Central Analysis Reporting"
addlabelstatus =add_labels(page_id, labeltouse)
if page_id != 'Error':
    child_page_id = createPage("1. Database Analysis - " + page_name, page_id, 'CentralDBAnalysis.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("2. SQL Trend - " + page_name, page_id, 'CentralSqlTrend.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("3. Additional Info - " + page_name, page_id, 'CentralChart.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("4. Database Activity - " + page_name, page_id, 'CentralDBActivity.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("5. Sql Analysis - " + page_name, page_id, 'CentralSQLComp.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("6. Load Comparison - " + page_name, page_id, 'CentralLoadComp.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    child_page_id = createPage("7. All SQL Comparison - " + page_name, page_id, 'CentralAllSQLComp.txt')
    addlabelstatus =add_labels(child_page_id, labeltouse)
    print(f"Confluence page created as https://confluence.bskyb.com/display/nonfuntst/DB+Analysis+Summary+-+{page_name.replace(' ', '+')} ")
###################################################################################