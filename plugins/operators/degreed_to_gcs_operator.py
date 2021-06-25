from tempfile import NamedTemporaryFile
import logging
import json
from datetime import datetime, timedelta

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, SkipMixin
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


from hooks.degreed_hook import DegreedHook


class DegreedToCloudStorageOperator(BaseOperator, SkipMixin):
    """
    Github To Cloud Storage Operator
    :param degreed_conn_id:          The Degreed connection id.
    :type github_conn_id:            string
    :param degreed_object:            The desired Github object. The currently
                                     supported values are:
                                        - logins
                                        - users
                                        - completions
                                        - required-learning
                                        - views
    :type degreed_object:             string
    :param payload:                  The associated degreed parameters to
                                     pass into the object request as
                                     keyword arguments.
    :type payload:                   dict
    :param destination:              The final destination where the data
                                     should be stored. Possible values include:
                                        - GCS                           
    :type destination:               string
    :param dest_conn_id:             The destination connection id.
    :type dest_conn_id:              string
    :param bucket:                   The bucket to be used to store the data.
    :type bucket:                    string
    :param key:                      The filename to be used to store the data.
    :type key:                       string
    """


    template_fields = ('payload',
                       'gcs_key',
                       'start_at',
                       'end_at')


    def __init__(self,
                 degreed_conn_id,
                 endpoint,
                 gcs_conn_id,
                 gcs_bucket,
                 gcs_key,
                 output_format='json',
                 start_at=None,
                 end_at=None,
                 payload={},
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.degreed_conn_id = degreed_conn_id
        self.endpoint = endpoint.lower()
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_key = gcs_key
        self.output_format = output_format.lower()
        self.start_at = start_at
        self.end_at = end_at
        self.payload = payload

        if self.endpoint.lower() not in ('logins',
                                         'users',
                                         'completions',
                                         'views',
                                         'required-learning',
                                         'pathways'):

            raise Exception('Specified Degreed object not currently supported.')

    def execute(self, context):
        h = DegreedHook(self.degreed_conn_id)
        print(h.text)
        # self.token = (DegreedHook(http_conn_id=self.degreed_conn_id)
        #                 .run(self.methodMapper('auth'))
        #                 .json())['access_token']
        
        # print(self.token)


    def methodMapper(self, endpoint):
        """
        This method maps the desired object to the relevant endpoint.
        """
        mapping = {"users": "api.degreed.com/api/v2/users",
                   "logins": "api.degreed.com/api/v2/logins"
                   }

        return mapping[endpoint]

    #     if self.endpoint == 'logins':
    #         paging_token = self.paginate_data(endpoint='paging_token',
    #                                          payload={'sinceDatetime': '2018-01-01T00:00:00'})


    # def get_url(self, endpoint, ref_date=datetime.now() + timedelta(days=-1), number_days=1):
    #     """
    #     This method generates the desired url to the relevant endpoint.
    #     Default date range set is previous day
    #     """
    #     urls = []
    
    #     if endpoint in ['auth']:
    #         urls = "https://degreed.com/oauth/token"

    #     elif endpoint in ['pathways', 'users']:
    #         urls.append(f"https://api.degreed.com/api/v2/{endpoint}?limit=1000")

    #     else:
    #         for i in range(0,number_days):
    #             date = ref_date + timedelta(days=-i)
    #             start_at = date.strftime("%Y-%m-%d")
    #             end_at = date.strftime("%Y-%m-%d")
                
    #             urls.append(f"https://api.degreed.com/api/v2/{endpoint}?filter[start_at]={start_at}&filter[end_at]={end_at}&limit=1000")

    #     return urls

    
    # def get_data(self, endpoint=None, payload=None):
    #     if not endpoint:
    #         endpoint = self.endpoint

    #     def make_request(http_conn_id,
    #                      endpoint,
    #                      payload=None,
    #                      token=None):
            
    #         return (DegreedHook(http_conn_id=http_conn_id)
    #                 .run(endpoint, payload, token=token)
    #                 .json())
        
    #     final_payload = {}

    #     for param in self.payload:
    #         final_payload[param] = self.payload[param]

    #     if payload:
    #         for param in payload:
    #             final_payload[param] = payload[param]