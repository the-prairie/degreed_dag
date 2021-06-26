import time
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests
from airflow.models import Variable


CLIENT_ID = Variable.get('client_id')
CLIENT_SECRET = Variable.get('client_secret')


class DegreedHook:
    
    SECONDS_BEFORE_EXPIRY = 60
    
    def __init__(self):
        self.token_url = 'https://degreed.com/oauth/token'
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET


        if not (self.client_id and self.client_secret):
            raise ValueError("Parameters 'client_id' and 'client_secret' have to be set in order "
                             "to authenticate with Degreed API service")

        self._token = None
        _ = self.token
 

    @property
    def token(self):
        """ Always up-to-date session's token
        :return: A token in a form of dictionary of parameters
        :rtype: dict
        """
        if self._token and self._token['expires_at'] > time.time() + self.SECONDS_BEFORE_EXPIRY:
            return self._token

        # A request parameter is created only in order for error handling decorators to work correctly
        request = self.token_url
        self._token = self._fetch_token(request)

        return self._token

    @property
    def session_headers(self):
        """ Provides session authorization headers
        :return: A dictionary with authorization headers
        :rtype: dict
        """
        return {
            'Authorization': 'Bearer {}'.format(self.token['access_token'])
        }
    

    def _fetch_token(self, request):
        """ Collects a new token from Degreed API service
        """

        oauth_client = BackendApplicationClient(client_id=self.client_id)

        with OAuth2Session(client=oauth_client) as oauth_session:
            return oauth_session.fetch_token(
                token_url=request,
                client_id=self.client_id,
                client_secret=self.client_secret,
                scope = 'xapi:read users:read logins:read pathways:read completions:read views:read skill_plans:read shared_items:read'
            )
    
    def run(self, endpoint, data=None, headers=None):
        headers = self.session_headers
        
        return requests.get(url=endpoint, data=data, headers=headers)

