import pandas as pd

from dagster import ConfigurableResource

class FilterResource(ConfigurableResource):
    def filter_by_client(self, client: str, df: pd.DataFrame):
        filtered_df = df[df['customer_source'] == client]
        return filtered_df

