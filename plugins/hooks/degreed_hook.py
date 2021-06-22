from airflow.hooks.http_hook import HttpHook

class DegreedHook(HttpHook):
    """
     Add OAuth support to the basic HttpHook
     """
    def __init__(self, method='POST', http_conn_id='degreed_default'):
        self.connection = self.get_connection(http_conn_id)
        self.CLIENT_ID = self.connection.extra_dejson.get('client_id')
        self.CLIENT_SECRET = self.connection.extra_dejson.get('client_secret')
        super().__init__(method, http_conn_id)

    def run(self,
            endpoint,
            data=None,
            headers=None,
            extra_options=None,
            token=None):
        self.endpoint = endpoint

        if endpoint == 'degreed.com/oauth/token':
            data = {"grant_type": "client_credentials",
                    "client_id": "{0}".format(self.CLIENT_ID),
                    "client_secret": "{0}".format(self.CLIENT_SECRET),
                    "scope": "users:read logins:read pathways:read completions:read views:read required-learning:read"
                    }
            method = 'POST'
        else:
            headers = {"Authorization": "Bearer {0}".format(token),
                       "Content-Type": "application/json"}
            method = 'GET'
        return super().run(endpoint, data, headers, extra_options)







# from typing import Any, Dict, Optional

# import oauthlib.oauth2 as oauth2
# import requests
# import requests_oauthlib
# from airflow.hooks.http_hook import HttpHook

# OptionalDictAny = Optional[Dict[str, Any]]


# class DegreedHook(HttpHook):
#     """
#     Add OAuth support to the basic HttpHook
#     """

#     def __init__(self, token_url="https://degreed.com/oauth/token", *args, **kwargs) -> None:
#         super().__init__(*args, **kwargs)
#         self.token_url = token_url

#     # headers may be passed through directly or in the "extra" field in the connection
#     # definition

#     def get_conn(self, headers: OptionalDictAny = None) -> requests_oauthlib.OAuth2Session:
#         conn = self.get_connection(self.http_conn_id)

#         # login and password are required
#         assert conn.login and conn.password

#         client = oauth2.BackendApplicationClient(client_id=conn.login)
#         session = requests_oauthlib.OAuth2Session(client=client)

#         # inject token to the session
#         assert self.token_url
#         session.fetch_token(
#             token_url=self.token_url,
#             client_id=conn.login,
#             client_secret=conn.password,
#             scope = 'users:read logins:read pathways:read completions:read views:read required-learning:read',
#             include_client_id=True
#         )
#         if conn.host and "://" in conn.host:
#             self.base_url = conn.host
#         else:
#             # schema defaults to HTTPS
#             schema = conn.schema if conn.schema else "https"
#             host = conn.host if conn.host else ""
#             self.base_url = schema + "://" + host

#         if conn.port:
#             self.base_url = self.base_url + ":" + str(conn.port)

#         if conn.extra:
#             try:
#                 session.headers.update(conn.extra_dejson)
#             except TypeError:
#                 self.log.warn('Connection to %s has invalid extra field.', conn.host)
#         if headers:
#             session.headers.update(headers)

#         return session








# # from airflow.hooks.http_hook import HttpHook


# # class DegreedHook(HttpHook):

# #     def __init__(self, method='GET', http_conn_id='http_default'):
# #         self.connection = self.get_connection(http_conn_id)
# #         self.CLIENT_ID = self.connection.extra_dejson.get('client_id')
# #         self.CLIENT_SECRET = self.connection.extra_dejson.get('client_secret')
# #         super().__init__(method, http_conn_id)

# #     def run(self,
# #             endpoint,
# #             data=None,
# #             headers=None,
# #             extra_options=None,
# #             token=None):
# #         self.endpoint = endpoint

# #         if endpoint == 'https://degreed.com/oauth/token':
# #             data = {"grant_type": "client_credentials",
# #                     "client_id": "{0}".format(self.CLIENT_ID),
# #                     "client_secret": "{0}".format(self.CLIENT_SECRET)}
# #         else:
# #             headers = {"Authorization": "Bearer {0}".format(token),
# #                        "Content-Type": "application/json"}
# #         return super().run(endpoint, data, headers, extra_options)
