import dagster as dg
import requests

from dagster import ConfigurableResource
from ...configs import config

class EmailResource(ConfigurableResource):
    def sent_email(self, client: str, message: str):
        CLIENT_CHANNEL = f'{client.upper()}_CHANNEL'
        URL = getattr(config, CLIENT_CHANNEL)
        requests.post(URL, data=message.encode(encoding='utf-8'))

