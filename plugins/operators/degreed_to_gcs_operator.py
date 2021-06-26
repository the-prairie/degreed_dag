from tempfile import NamedTemporaryFile
import logging
from urllib.parse import urlencode
import json
from datetime import datetime, timedelta
from flatten_json import flatten
import pandas as pd

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, SkipMixin
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


from hooks.degreed_hook import DegreedHook


class DegreedToCloudStorageOperator(BaseOperator, SkipMixin):
    """
    Github To Cloud Storage Operator
    
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
                 endpoint,
                 gcs_conn_id,
                 gcs_bucket,
                 gcs_key,
                 output_format='csv',
                 start_at=None,
                 end_at=None,
                 payload={},
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
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
        h = DegreedHook()
        base_url = "https://api.degreed.com/api/v2"
        output = []

        if self.endpoint  in ('users'):   
            params = {'limit': 1000}
            params['filter[start_date]'] = None
            params['filter[end_date]'] = None
        
        elif self.endpoint in ('logins'):    
            params = {'limit': 1000}
            params['filter[start_date]'] = self.start_at
            params['filter[end_date]'] = self.end_at


        
        response_body = h.run(endpoint="{0}/{1}".format(base_url, self.endpoint), params=params).json()
        if not response_body:
            logging.info('Resource Unavailable.')

        while 'next' in response_body.get('links', {}):
            output.extend(response_body.get('data'))
            response = h.run(response_body.get('links')['next'])
            response.raise_for_status()
            response_body = response.json()

        output.extend(response_body.get('data'))

        df = pd.DataFrame([flatten(x) for x in output])

        df.to_csv(self.gcs_key, index=False)

        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)

        hook.upload(bucket=self.gcs_bucket,
                object=self.gcs_key,
                filename=self.gcs_key,
                mime_type='text/plain')



