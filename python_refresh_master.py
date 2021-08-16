# %% imports
from datetime import datetime


import sheets_connector as shc
import update_bot
# %% load objects
bot = update_bot.update_bot()
sheets_api = shc.SheetsAPI()
# %% load data from refresh bot master file


def run_updates_from_master(worksheet_name, sheets_id='1CgC_6OFEreB_qyQAOqJqO1VN_mApXnFXYrZrxM3Il4A'):
    cell_range_insert = 'B6:G100'
    sheets_to_refresh = sheets_api.ReadFromSheets(
        sheets_id, worksheet_name, cell_range_insert=cell_range_insert, header=True)
    sheets_standar_column = {
        'sheets_id': 5,
        'comma_as_decimal': 3,
        'queries_tab': 4
    }
    # %% run refreshes on sql
    update_timer = []
    for row in sheets_to_refresh.values:
        print('\nUpdating {} @ {}\n'.format(row[1], row[2]))
        bot.bot(sheets_id=row[sheets_standar_column['sheets_id']], is_comma=row[sheets_standar_column['comma_as_decimal']], base_ws_name=row[sheets_standar_column['queries_tab']]
                )
        update_timer.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    sheets_to_refresh['update_ended_at'] = update_timer
    sheets_api.InsertToSheets(sheets_id, sheets_to_refresh, worksheet_name,
                              cell_range_insert=cell_range_insert.split(':')[0])


# %%
if(__name__ == "__main__"):
    run_updates_from_master('monday_morning')
# %%
