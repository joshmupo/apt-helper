import os.path
import requests
import sys
import calendar
import logging
import argparse
import concurrent.futures
import json

from datetime import date

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]
API_KEY = None
SHEET_API_BASE_URL = 'https://sheets.googleapis.com/v4/spreadsheets'
DRIVE_API_BASE_URL = 'https://www.googleapis.com/drive/v3'
CONVERTED_DATE = None
# NEW_SPREADSHEET_URL = None

# Thai abbreviated month names
thai_abbreviated_month_names = [
    "มค.", "กพ.", "มีค.", "เมย.", "พค.", "มิย.",
    "กค.", "สค.", "กย.", "ตค.", "พย.", "ธค."
]


def main(args):
    """Shows basic usage of the People API.
  Prints the name of the first 10 connections.
  """
    # setup basic logger
    logging.basicConfig(level=logging.ERROR)

    # setup creds to query google apis
    creds = getCreds()

    # set api key from apikey.json
    setAPIKey()

    # set our formatted date globally
    setDate(args)

    # dynamically pull the spreadsheet id based on sheet name
    original_spreadsheet_id = getStartingSpreadsheetID(creds)
    new_spreadsheet_id = copySpreadsheet(creds, original_spreadsheet_id)

    # use id of new spreadsheet to update it
    updateSpreadsheetData(creds, new_spreadsheet_id)


def printSheetData(data):
    logging.info(data)
    sheet_data = data.get('sheets', [])
    for sheet in sheet_data:
        logging.info('------------------------- Sheet Name: {} -----------'.format(sheet["properties"]["title"]))
        for data in sheet.get('data', []):
            for rowData in data.get('rowData', []):
                logging.info(rowData)


def updateSpreadsheetData(creds, spreadsheet_id):
    sheets_api_url = f'{SHEET_API_BASE_URL}/{spreadsheet_id}?includeGridData=false&key={API_KEY}'
    sheets_response = doGetRequest(creds, sheets_api_url)
    sheets_data = sheets_response.get('sheets', [])

    if len(sheets_data) < 1:
        logging.error(f'no sheets found under sheet id:{spreadsheet_id}')
        sys.exit(1)

    # Execute batches in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(processSheetData, creds, spreadsheet_id, sheet) for sheet in sheets_data]

        # Process result
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            logging.info(result)


def processSheetData(creds, spreadsheet_id, sheet):
    sheet_properties = sheet.get('properties', {})
    sheet_title = sheet_properties.get('title', '')
    try:
        # query sheet data for each sheet using range filter for data
        sheets_api_url = f'{SHEET_API_BASE_URL}/{spreadsheet_id}/values:batchGet?key={API_KEY}' \
                         f'&ranges={sheet_title}!C11&ranges={sheet_title}!C12' \
                         f'&ranges={sheet_title}!C13&ranges={sheet_title}!C10' \
                         f'&ranges={sheet_title}!I5&ranges={sheet_title}!C5'
        sheets_response = doGetRequest(creds, sheets_api_url)
        sheets_data = sheets_response.get('valueRanges', [])
        logging.info(f'TITLE: {sheet_title} SHEET DATA: {sheets_data}')
        # probably parse the data a little further to get the values we want
        for i, rangeData in enumerate(sheets_data):
            rangeComparator = rangeData['range']
            if "C11" in rangeComparator:
                logging.debug("updated c11 to e11")
                sheets_data[i]['range'] = sheets_data[i]['range'].replace("C11", "E11")
            elif "C12" in rangeComparator:
                logging.debug("updated c12 to e12")
                sheets_data[i]['range'] = sheets_data[i]['range'].replace("C12", "E12")
            elif "C13" in rangeComparator:
                logging.debug("updated c13 to e13")
                sheets_data[i]['range'] = sheets_data[i]['range'].replace("C13", "E13")
            elif "I5" in rangeComparator:
                sheets_data[i]['values'][0][0] = CONVERTED_DATE.strftime("%d/%m/%y")
            elif "C10" in rangeComparator:
                sheets_data[i]['values'][0][0] = thai_abbreviated_month_names[CONVERTED_DATE.month - 1] + \
                                                 CONVERTED_DATE.strftime("%y")
            elif "C5" in rangeComparator:
                # get last 3 nums and put mon/year in front
                last_three = sheets_data[i]['values'][0][0][-3:]
                gregorian_date = date(CONVERTED_DATE.year - 543, CONVERTED_DATE.month, CONVERTED_DATE.day)
                sheets_data[i]['values'][0][0] = gregorian_date.strftime("%y%m") + "".join(last_three)
        batch_update_payload = {
            'valueInputOption': 'USER_ENTERED',
            'data': sheets_data
        }

        batch_update_url = f'{SHEET_API_BASE_URL}/{spreadsheet_id}/values:batchUpdate'
        update_resp = doPostRequest(creds, batch_update_url, batch_update_payload)
        logging.debug(update_resp)

    except:
        logging.error(f"error occurred trying to get/update sheet with title: {sheet_title}")

    return f'Finished processing sheet: {sheet_title}'


def update_cell_value(creds, sheets_api_url, range, new_value):
    try:
        # Construct the request payload with the new value
        sheets_payload = {
            'values': [[new_value]]
        }

        # Make the API request to update the cell value
        response = doPutRequest(creds, sheets_api_url, sheets_payload)

        if response.status_code == 200:
            logging.info(f"Cell {range} updated successfully.")
        else:
            logging.error(f"Error updating cell value. Status code: {response.status_code} Body: {response.json()}")

    except Exception as e:
        logging.error(f"Error: {e}")


def getStartingSpreadsheetID(creds):
    prevMonthIndex = CONVERTED_DATE.month - 2
    if prevMonthIndex == -1:
        prevMonthIndex = 11
    prevMonth = thai_abbreviated_month_names[prevMonthIndex]
    original_spreadsheet_title = prevMonth + CONVERTED_DATE.strftime("%y")
    drive_spreadsheets = getDriveFilesByFilter(creds, f'name=\'{original_spreadsheet_title}\'')
    files = drive_spreadsheets.get('files', [])
    if len(files) < 1:
        logging.error(
            f'no files found in google drive matching the name for previous month: {original_spreadsheet_title}')
        sys.exit(1)
    # just take the first matching one if there's multiple
    original_spreadsheet_id = files[0]['id']
    logging.info(f'found matching spreadsheet with id: {original_spreadsheet_id}')
    return original_spreadsheet_id


def copySpreadsheet(creds, existing_spreadsheet_id):
    logging.info(f"starting to copy existing spreadsheet data with id: {existing_spreadsheet_id}")
    url = f'{DRIVE_API_BASE_URL}/files/{existing_spreadsheet_id}/copy'
    # whatever title is, current month
    body = {'name': f'{thai_abbreviated_month_names[CONVERTED_DATE.month - 1] + CONVERTED_DATE.strftime("%y")}'}
    res = doPostRequest(creds, url, body)
    new_spreadsheet_id = res['id']

    logging.info(f"New spreadsheet created with ID: {new_spreadsheet_id}")

    return new_spreadsheet_id


def getSpreadsheetData(creds, spreadsheet_id):
    get_url = '{api_base_url}/{spreadsheet_id}?includeGridData=false&key={api_key}'.format(
        api_base_url=SHEET_API_BASE_URL, spreadsheet_id=spreadsheet_id, api_key=API_KEY)
    return doGetRequest(creds, get_url)


def getDriveFilesByFilter(creds, query):
    url = f'{DRIVE_API_BASE_URL}/files?q={query}&key={API_KEY}'

    return doGetRequest(creds, url)


def doPostRequest(creds, url, body):
    try:
        headers = {
            'Authorization': f'Bearer {creds.token}',
            'Accept': 'application/json',
        }
        res = requests.post(url, headers=headers, json=body)
        if res.status_code == 200:
            return res.json()
        else:
            logging.error(f"Error creating data. Status code: {res.status_code} Body: {res.json()}")
    except HttpError as err:
        logging.error(err)


def doPutRequest(creds, url, body):
    try:
        headers = {
            'Authorization': f'Bearer {creds.token}',
            'Accept': 'application/json',
        }
        res = requests.put(url, headers=headers, json=body)
        if res.status_code == 200:
            return res
        else:
            logging.error(f"Error updating data. Status code: {res.status_code} Body: {res.json()}")
    except HttpError as err:
        logging.error(err)


def doGetRequest(creds, url):
    try:
        headers = {
            'Authorization': f'Bearer {creds.token}',
            'Accept': 'application/json',
        }
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            return res.json()
        else:
            logging.error(f"Error fetching data. Status code: {res.status_code} Body: {res.json()}")
    except HttpError as err:
        logging.error(err)


def getCreds():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds


def setAPIKey():
    if os.path.exists("apikey.json"):
        with open("apikey.json") as data:
            apikey = json.load(data)
            global API_KEY
            API_KEY = apikey['apikey']
    else:
        logging.error("missing apikey.json file")
        sys.exit(1)


def setDate(args):
    current_date = date.today()
    if args.override_month:
        current_date = date(current_date.year, args.override_month, current_date.day)
    last_day = calendar.monthrange(current_date.year, current_date.month)[1]
    global CONVERTED_DATE
    CONVERTED_DATE = date(current_date.year + 543, current_date.month, last_day)


if __name__ == "__main__":
    print("Starting...")

    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description="Generate a date in the Buddhist era with optional month override.")

    # Add an argument for the overridden month (default is the next month)
    parser.add_argument("--override-month", type=int, choices=range(1, 13), help="Override the month (1-12)")

    # Parse the command line arguments
    args = parser.parse_args()

    # Call everything
    main(args)

    print("Successfully completed")
