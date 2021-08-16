# %% imports

import sheets_connector as shc
import drive_connector as drc
import snowflake_connector as sfc

import codecs
from datetime import datetime
import sys

import pandas as pd
import numpy as np

# %% 
class upload_bot():
    def __init__(self):
        self.snowflake = sfc.SnoflakeAPI()
        # self.drive = drc.DriveAPI()
        self.sheets = shc.SheetsAPI()
        self.sql_types = {'integer':0,
                'boolean':0,
                'text':1,
                'date':1,
                'array':2}

# """insert into GLOBAL_GROWTH.BR_REACTIVATION_CAMPAIGNS_SCHEDULER 
# (ID, campaign_name, updated_at, BAU, Starts_at, Ends_at, tier, Recency_min, Recency_Max, city, microzone_id, CC, OS, Last_Digits, timeframe, is_prime, is_rappicredits, Reference_Store_IDs, Priority, Push_slot, WRITABLE)
# SELECT $1, $2, $3, $4, $5, $6, parse_json($7), $8, $9, parse_json($10), parse_json($11), $12, parse_json($13), parse_json($14), $15, $16, $17, parse_json($18), $19, $20, $21
# from values"""

    def bot(self,sheets_id,line_amount = 500):
        base_query = "insert into {} {} select {} from values {}"
        base_ws_name = 'Uploads'
        base_upload_ranges = 'B4:D100'
        tabs = self.sheets.ReadFromSheets(sheets_id, base_ws_name, base_upload_ranges, header = True)
        updates = []
        type_format = {
                0:" ${}",
                1:" ${}",
                2:" parse_json(${})"}
        select_format = {
                0:" {}",
                1:" '{}'",
                2:" $$[{}]$$"}
        
        for tab in tabs.values:
            df = self.sheets.ReadFromSheets(sheets_id, tab[0], tab[1],header = True)
            # return(df)
            data_types = df.columns
            df.columns = df.iloc[0]
            df = df[1:]
            header = "({})".format(','.join(df.columns))
            composition = []
            add_values = []

            for i, value_type in enumerate(data_types):
                composition.append(type_format[self.sql_types[value_type]].format(i+1))
            composition = ','.join(composition)

            for count, row in enumerate(df.values):
                line = []
                for j, value in enumerate(row):
                    line.append(select_format[self.sql_types[data_types[j]]].format(value))
                add_values.append('({})'.format(','.join(line)))
                if(count%line_amount == 0 and count != 0):
                    add_content = ','.join(add_values)
                    query = base_query.format(tab[2], header, composition, add_content)
                    self.snowflake.pandas_df_from_snowflake_query(query)
                    add_values = []
                    # print(count/line_amount)  
                    
                    # return(query)
            if(count%line_amount != 0 or count == 0):
                add_content = ','.join(add_values)
                query = base_query.format(tab[2], header, composition, add_content)
                # return(query)
                self.snowflake.pandas_df_from_snowflake_query(query) 
                                 
            # updates.append(sys.exc_info()[0])
            # updates.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        # tabs['Updates'] = updates
        # self.sheets.InsertToSheets(sheets_id,tabs,base_ws_name,base_upload_ranges.split(':')[0])
        

# %%
# if(__name__=="__main__"):
# https://docs.google.com/spreadsheets/d/1_csrWLVJ2PiigUg1eAMiIhOLtz1Sui_oZ_wdvIFp9-Q/edit#gid=0
bot = upload_bot()
sheets_id = '1_csrWLVJ2PiigUg1eAMiIhOLtz1Sui_oZ_wdvIFp9-Q'
bot.bot(sheets_id)

# %%
