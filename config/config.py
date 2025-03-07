import os

from taiga import TaigaAPI

from .enviroments_custom import TAIGA_API_URL, TAIGA_PASSWORD, TAIGA_USERNAME


def authenticate():
    api = TaigaAPI(
        host=TAIGA_API_URL,
        tls_verify=True,
    )
    api.auth(username=TAIGA_USERNAME, password=TAIGA_PASSWORD)
    return api