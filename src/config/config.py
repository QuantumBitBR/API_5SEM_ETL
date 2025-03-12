import os

from taiga import TaigaAPI

from .enviroments_custom import TAIGA_API_URL, TAIGA_PASSWORD, TAIGA_USERNAME


class TaigaClient:
    """TaigaClient is a singleton class that provides a single instance of the Taiga API."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TaigaClient, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initializes the attributes and authenticates the API."""
        self.api_url = TAIGA_API_URL
        self.username = TAIGA_USERNAME
        self.password = TAIGA_PASSWORD
        self.api = self.authenticate()

    def authenticate(self):
        """Authenticates and returns the API instance."""
        api = TaigaAPI(host=self.api_url, tls_verify=True)
        api.auth(username=self.username, password=self.password)
        return api
