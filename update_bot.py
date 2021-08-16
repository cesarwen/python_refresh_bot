# %% imports

import sheets_connector as shc
import drive_connector as drc
import snowflake_connector as sfc

import codecs
from datetime import datetime
import sys
import os
import glob

import pandas as pd
import numpy as np

# %%


class update_bot():
    def __init__(self):
        self.snowflake = sfc.SnoflakeAPI()
        self.drive = drc.DriveAPI()
        self.sheets = shc.SheetsAPI()

    def bot(self, sheets_id, is_comma=False, base_ws_name='Queries'):
        base_folder_id_cell = 'C4'
        base_query_ranges = 'B6:D100'
        queries_folder = 'queries'
        folder_id = self.sheets.ReadFromSheets(
            sheets_id, base_ws_name, base_folder_id_cell).values[0][0]
        self.drive.FolderDownload(folder_id, queries_folder)
        files = self.sheets.ReadFromSheets(
            sheets_id, base_ws_name, base_query_ranges, header=True)
        updates = []
        for value in files.values:
            try:
                print("{} - {} - {}".format(value[0], value[1], value[2]))
                with codecs.open('{}//{}'.format(queries_folder, value[0]), encoding='utf-8') as f:
                    query = f.read()
                    raw_df = self.snowflake.pandas_df_from_snowflake_query(
                        query)
                    self.sheets.InsertToSheets(
                        sheets_id, raw_df, value[1], value[2], use_comma=is_comma)
                    updates.append(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except:
                print("Unexpected error:", sys.exc_info())
                updates.append(sys.exc_info()[0])
        files['Updates'] = updates
        self.sheets.InsertToSheets(
            sheets_id, files, base_ws_name, base_query_ranges.split(':')[0], use_comma=False)
        for value in (glob.glob('.\\{}\\*sql'.format(queries_folder))):
            if os.path.exists('{}'.format(value)):
                os.remove('{}'.format(value))


# %%
if(__name__ == "__main__"):
    bot = update_bot()
    bot.bot('1gpqhngGeyyZu1X628-Zzb2PFp57OFdzdfXc6-2tLO3A', is_comma=True)
