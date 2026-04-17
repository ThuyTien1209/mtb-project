import dagster as dg
import pandas as pd
import gspread
import datetime as dt

from dagster import ConfigurableResource
from .filter import FilterResource
from ...configs import config

class ClientResource(ConfigurableResource):

    def filter_by_client(self, client: str, df: pd.DataFrame):
        filtered_df = df[df['customer_source'] == client]
        return filtered_df
    
    def get_client(self, client: str, df: pd.DataFrame):

        FILTER_DF = self.filter_by_client(client, df)

        try:
            gc = gspread.service_account(filename=config.GC_KEY)
            GDATA = gc.open_by_key(config.GSHEET_KEY)
            METABASE_GSHEET = GDATA.worksheet(client)
        except gspread.exceptions.WorksheetNotFound:
            METABASE_GSHEET = GDATA.add_worksheet(title = client, rows=FILTER_DF.shape[0], cols=FILTER_DF.shape[1])
            
        METABASE_GSHEET.clear()
        METABASE_GSHEET.update([FILTER_DF.columns.values.tolist()] + FILTER_DF.values.tolist())

        MIN_DATE = FILTER_DF['created_at'].min()
        MAX_DATE = FILTER_DF['created_at'].max()
        DF_ROWS = FILTER_DF.shape[0]

        return MIN_DATE, MAX_DATE, DF_ROWS




