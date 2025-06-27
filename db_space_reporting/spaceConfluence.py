#!/usr/bin/python
###################################################################################
# Name : spaceConfluence.py
# Purpose : Create confluence page for space
#     Called by jenkins job https://ppejenkins.bskyb.com/view/NFT%20DBA/view/NFT%20DBA%20-%20Analysis%20Jobs/job/NFT%20DBA%20-%20AWR%20Report%20Generation/
#     Based on https://stackoverflow.com/questions/33168060/how-can-i-create-a-new-page-to-confluence-with-python
###################################################################################
# Change History :
#     27-Dec-2023 Alex Hyslop initial version
###################################################################################
# initialize
import requests
import json
import datetime
from requests.auth import HTTPBasicAuth
# Global variables: set auth token and get the basic auth code
auth_token = 'nftauto'
basic_auth = HTTPBasicAuth('nft-upload', auth_token)
space_key = 'nonfuntst'  # 'UK IS Platform Performance Engineering'
url = 'https://confluence.bskyb.com/rest/api/content/'
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
deletePageByTitle("N01 Database Growth");
page_id = createPage("N01 Database Growth", 198648455, 'space.txt')  # 198648455 is 'NFT Database Space' page.
print("N01 Database Growth Page created under https://confluence.bskyb.com/display/nonfuntst/NFT+Database+Space")

deletePageByTitle("N01 Schema Growth");
page_id = createPage("N01 Schema Growth", 198648455, 'schema_space.txt')  # 198648455 is 'NFT Database Space' page.
print("N01 Schema Growth Page created under https://confluence.bskyb.com/display/nonfuntst/NFT+Database+Space")

deletePageByTitle("N01 Tablespace Growth");
page_id = createPage("N01 Tablespace Growth", 198648455, 'ts_space.txt')  # 198648455 is 'NFT Database Space' page.
print("N01 Tablespace Growth Page created under https://confluence.bskyb.com/display/nonfuntst/NFT+Database+Space")
###################################################################################
