import dagster as dg
import json
import pandas as pd
import numpy as np
import requests
import datetime as dt
import gspread

from metabase_query import Metabase
from ...configs import config
from ..resources.notifier import NftyResource

TODAY = dt.date.today().strftime('%Y-%m-%d')

@dg.asset
def metabase_data(context: dg.AssetExecutionContext,
                  nfty: NftyResource) -> pd.DataFrame: 
    context.log.info(f'Bat dau lay du lieu tu metabase ngay {TODAY}')

    try:
        mb = Metabase(metabase_session=config.METABASE_SESSION)
        url = config.URL
        data = mb.query(url=url, format='json')

        SUCCESS = f'Da lay du lieu tu metabase thanh cong'
        context.log.info(SUCCESS)
        nfty.success(SUCCESS)

    except Exception as e:
        ERROR = f'Loi lay du lieu tu metabase ngay {TODAY}: {str(e)}'
        context.log.info(ERROR + '- Dang gui loi ve ntfy')
        nfty.failure(ERROR)
        context.log.info('Da gui loi ve ntfy')
 
    
    context.log.info('Dang chuyen doi du lieu tu metabase sang DataFrame')

    try:
        df = pd.json_normalize(data)

        SUCCESS = f'Da chuyen doi du lieu tu metabase sang DataFrame thanh cong'
        context.log.info(SUCCESS)
        # nfty.success(SUCCESS)

    except Exception as e:
        ERROR = f'Loi chuyen doi du lieu tu metabase sang DataFrame ngay {TODAY}: {str(e)}'
        context.log.info(ERROR + '- Dang gui loi ve ntfy')
        nfty.failure(ERROR)
        context.log.info('Da gui loi ve ntfy')

    return df


@dg.asset(deps=['metabase_data'])
def cleaned_data(context: dg.AssetExecutionContext,
                 nfty: NftyResource,
                 metabase_data: pd.DataFrame) -> pd.DataFrame:
    context.log.info('Bat dau lam sach du lieu')
    
    try: 
        df = metabase_data.copy()

        # Missing values
        df['updated_at'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        context.log.info('Xu ly missing value')

        df['created_at'] = pd.to_datetime(df['created_at'], utc=True, format='ISO8601')
        for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            df[col] = df[col].fillna("Unknown")

        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].fillna("Unknown")
        for col in df.select_dtypes(include=['float', 'int']):
            df[col] = df[col].fillna(0)
      
        SUCCESS = 'Lam sach du lieu thanh cong'
        context.log.info(SUCCESS)
        nfty.success(SUCCESS)
    
    except Exception as e:
        ERROR = f'Loi lam sach du lieu ngay {TODAY}: {str(e)}'
        context.log.info(ERROR + '- Dang gui loi ve ntfy')
        nfty.failure(ERROR)
        context.log.info('Da gui loi ve ntfy')
    
    return df


@dg.asset(deps=['cleaned_data'])
def gsheet_data(context: dg.AssetExecutionContext,
                nfty: NftyResource,
                cleaned_data: pd.DataFrame) -> None:
    context.log.info('Bat dau load du lieu vao Google Sheet')

    try: 
        df = cleaned_data.copy()

        MIN_DATE = df["created_at"].min()
        MAX_DATE = df["created_at"].max()
        
        gc = gspread.service_account(filename=config.GC_KEY)
        GDATA = gc.open_by_key(config.GSHEET_KEY)
        METABASE_GSHEET = GDATA.worksheet('Sheet1')

        METABASE_GSHEET.clear() # replace old data
        METABASE_GSHEET.update([df.columns.values.tolist()] + df.values.tolist())
    
        # gui noti ve shape df, range data va updated_at
        SUCCESS = f'Da load {df.shape[0]} dong du lieu vao Google Sheet thanh cong. Range data: {MIN_DATE} - {MAX_DATE}. Updated at: {dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        
        context.log.info(SUCCESS)
        nfty.success(SUCCESS)
    
    except Exception as e:
        ERROR = f'Loi load du lieu vao Google Sheet ngay {TODAY}: {str(e)}'
        context.log.info(ERROR + '- Dang gui loi ve ntfy')
        nfty.failure(ERROR)
        context.log.info('Da gui loi ve ntfy')


##################################
# ASSET FOR EACH CLIENT SOURCE
##################################

@dg.asset(deps=['cleaned_data'])
def gsheet_data_by_client(context: dg.AssetExecutionContext,
                          nfty: NftyResource,
                          cleaned_data: pd.DataFrame) -> None:
    pass