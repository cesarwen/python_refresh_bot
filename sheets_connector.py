# import the required libraries
from __future__ import print_function
import pickle
import os.path
import pandas as pd
from mimetypes import MimeTypes
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import sys
  
class SheetsAPI:
    global SCOPES
      
    # Define the scopes
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
  
    def __init__(self):

        service_name = 'sheets'
        api_version = 'v4'

        # Variable self.creds will
        # store the user access token.
        # If no valid token found
        # we will create one.
        self.creds = None
  
        # The file token.pickle stores the
        # user's access and refresh tokens. It is
        # created automatically when the authorization
        # flow completes for the first time.
  
        # Check if file token.pickle exists
        if os.path.exists(f'token_{service_name}_{api_version}.pickle'):
            
            # Read the token from the file and
            # store it in the variable self.creds
            with open(f'token_{service_name}_{api_version}.pickle', 'rb') as token:
                self.creds = pickle.load(token)
  
        # If no valid credentials are available,
        # request the user to log in.
        if not self.creds or not self.creds.valid:
            # If token is expired, it will be refreshed,
            # else, we will request a new one.
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                self.creds = flow.run_local_server()
  
            # Save the access token in token.pickle
            # file for future usage
            with open(f'token_{service_name}_{api_version}.pickle', 'wb') as token:
                pickle.dump(self.creds, token)
        self.service = build(service_name, api_version, credentials=self.creds)
  
    def InsertToSheets(self, 
                    spreadsheet_id,
                    df,
                    worksheet_name,
                    cell_range_insert = 'A1',
                    major_dimension = 'ROWS',
                    input_options = 'USER_ENTERED',
                    use_comma = False):
        worksheet_name += "!"
        columns = df.columns
        for column in columns:
            df[column]= df[column].map(str)
            if (use_comma):
                df[column]= df[column].str.replace('.',',')
    
        try:
            # mySpreadsheets = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            values = df.T.reset_index().T.values.tolist()
            value_range_body = {
                'majorDimension': major_dimension,
                'values': values
            }
            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                valueInputOption=input_options,
                range=worksheet_name + cell_range_insert,
                body=value_range_body
            ).execute()
            return True
        except:
            
            # Return False if something went wrong
            print("Something went wrong.")
            return False
  
    def ReadFromSheets(self, 
                    spreadsheet_id,
                    worksheet_name,
                    cell_range_insert = 'A1',
                    header = False):
  
        try:
            sheet = self.service.spreadsheets()
            worksheet_name += "!"
            result = sheet.values().get(spreadsheetId=spreadsheet_id,range=worksheet_name + cell_range_insert).execute()
            values = result.get('values', [])
            df = pd.DataFrame(values)
            
            if(header):
                df.columns = df.iloc[0] #set the header row as the df header
                df = df[1:] #take the data less the header row
            df.dropna
            # df.columns = df.values[0]
            # df = df.drop(0)
            return(df)
        except:
            print(sys.exc_info()[0])
            # Raise UploadError if file is not uploaded.
            print("Something went wrong.")
  
if __name__ == "__main__":
    obj = SheetsAPI()
    df = pd.DataFrame([[1,2,3],[4,5,6]])
    obj.InsertToSheets('1_0ZvEUSl8Y5kRc4V9blYIMhAmUWgQYYiVNuN7pPrDyk',df,'Queries','B21')
    # print("this {}".format(obj.ReadFromSheets('1_0ZvEUSl8Y5kRc4V9blYIMhAmUWgQYYiVNuN7pPrDyk','Queries','B6:D12',True)))
