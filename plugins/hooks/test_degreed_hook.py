import pytest
from airflow.hooks.http_hook import HttpHook
from typing import Any, Dict, Optional

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests