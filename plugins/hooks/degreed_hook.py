from airflow.hooks.http_hook import HttpHook
from typing import Any, Dict, Optional

import oauthlib.oauth2 as oauth2
import requests
import requests_oauthlib

OptionalDictAny = Optional[Dict[str, Any]]

class DegreedHook(HttpHook):
    """
    Add OAuth support to the basic HttpHook
    """

    def __init__(self, degreed_conn_id, token_url="https://degreed.com/oauth/token", *args, **kwargs) -> None:
        super().__init__(http_conn_id=degreed_conn_id, *args, **kwargs)
        self.token_url = token_url
 
    # headers may be passed through directly or in the "extra" field in the connection
    # definition

    def get_conn(self, headers: OptionalDictAny = None) -> requests_oauthlib.OAuth2Session:
        conn = self.get_connection(self.http_conn_id)

        # login and password are required
        assert conn.login and conn.password

        client = oauth2.BackendApplicationClient(client_id=conn.login)
        session = requests_oauthlib.OAuth2Session(client=client)

        # inject token to the session
        assert self.token_url
        session.fetch_token(
            token_url=self.token_url,
            client_id=conn.login,
            client_secret=conn.password,
            scope = 'users:read logins:read pathways:read completions:read views:read required-learning:read',
            include_client_id=True
        )
        if conn.host and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTPS
            schema = conn.schema if conn.schema else "https"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port)

        if conn.extra:
            try:
                session.headers.update(conn.extra_dejson)
            except TypeError:
                self.log.warn('Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session
    
    def run(self,
            endpoint: str,
            data: OptionalDictAny = None,
            headers: OptionalDictAny = None,
            extra_options: OptionalDictAny = None) -> requests.Response:

        session = self.get_conn(headers)

        if self.base_url and not self.base_url.endswith('/') and \
           endpoint and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = (self.base_url or '') + (endpoint or '')

        req_settings = dict(
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify", False),
            proxies=extra_options.get("proxies", {}),
            cert=extra_options.get("cert"),
            timeout=extra_options.get("timeout"),
            allow_redirects=extra_options.get("allow_redirects", True)
        )

        try:
            self.log.info("Sending '%s' to url: %s", self.method, url)
            if self.method == 'GET':
                # GET uses params
                response = session.request(self.method,
                                           url,
                                           params=data,
                                           headers=headers,
                                           **req_settings)
            elif self.method == 'HEAD':
                # HEAD doesn't use params
                response = session.request(self.method,
                                           url,
                                           headers=headers,
                                           **req_settings)
            else:
                # Others use data
                response = session.request(self.method,
                                           url,
                                           data=data,
                                           headers=headers,
                                           **req_settings)

        except requests.exceptions.ConnectionError as ex:
            self.log.warning(str(ex) + ' Tenacity will retry to execute the operation')
            raise ex

        if extra_options.get('check_response', True):
            self.check_response(response)

        return response




# class DegreedHook(HttpHook):
#     """
#      Add OAuth support to the basic HttpHook
#      """
#     def __init__(self, method='POST', http_conn_id='degreed_default'):
#         self.connection = self.get_connection(http_conn_id)
    
#         self.CLIENT_ID = self.connection.extra_dejson.get('client_id')
#         self.CLIENT_SECRET = self.connection.extra_dejson.get('client_secret')
#         super().__init__(method, http_conn_id)

#     def run(self,
#             endpoint,
#             data=None,
#             headers=None,
#             extra_options=None,
#             token=None):
#         self.endpoint = endpoint

#         if endpoint == 'degreed.com/oauth/token':
#             data = {"grant_type": "client_credentials",
#                     "client_id": f"{self.CLIENT_ID}",
#                     "client_secret": f"{self.CLIENT_SECRET}",
#                     "scope": "users:read logins:read pathways:read completions:read views:read required-learning:read"
#                     }
#             method = 'POST'
#             endpoint = None
#         else:
#             headers = {"Authorization": "Bearer {0}".format(token),
#                        "Content-Type": "application/json"}
#             method = 'GET'
#         return super().run(endpoint, data, headers, extra_options)







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
