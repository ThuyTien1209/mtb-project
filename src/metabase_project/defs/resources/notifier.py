import dagster as dg
import requests

from dagster import ConfigurableResource
from ...configs import config

class NftyResource(ConfigurableResource):
    def success(self, message: str) -> None:
        requests.post(config.SUCCESS_CHANNEL, data=message.encode(encoding='utf-8'))

    def failure(self, message: str) -> None:
        requests.post(config.FAILURE_CHANNEL, data=message.encode(encoding='utf-8'))
    