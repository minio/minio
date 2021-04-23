# -*- coding: utf-8 -*-
import json
# standard.
import os

import certifi
# Dependencies
import urllib3
from botocore.credentials import CredentialProvider, RefreshableCredentials
from botocore.exceptions import CredentialRetrievalError
from dateutil.parser import parse

from .sts_element import STSElement


class ClientGrantsCredentialProvider(CredentialProvider):
    """
    ClientGrantsCredentialProvider implements CredentialProvider compatible
    implementation to be used with boto_session
    """
    METHOD = 'assume-role-client-grants'
    CANONICAL_NAME = 'AssumeRoleClientGrants'

    def __init__(self, cid, csec,
                 idp_ep='http://localhost:8080/auth/realms/minio/protocol/openid-connect/token',
                 sts_ep='http://localhost:9000'):
        self.cid = cid
        self.csec = csec
        self.idp_ep = idp_ep
        self.sts_ep = sts_ep

        # Load CA certificates from SSL_CERT_FILE file if set
        ca_certs = os.environ.get('SSL_CERT_FILE')
        if not ca_certs:
            ca_certs = certifi.where()

        self._http = urllib3.PoolManager(
            timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
            maxsize=10,
            cert_reqs='CERT_NONE',
            ca_certs=ca_certs,
            retries=urllib3.Retry(
                total=5,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504]
            )
        )

    def load(self):
        """
        Search for credentials with client_grants
        """
        if self.cid is not None:
            fetcher = self._create_credentials_fetcher()
            return RefreshableCredentials.create_from_metadata(
                metadata=fetcher(),
                refresh_using=fetcher,
                method=self.METHOD,
            )
        else:
            return None

    def _create_credentials_fetcher(self):
        method = self.METHOD

        def fetch_credentials():
            # HTTP headers are case insensitive filter out
            # all duplicate headers and pick one.
            headers = {}
            headers['content-type'] = 'application/x-www-form-urlencoded'
            headers['authorization'] = urllib3.make_headers(
                basic_auth='%s:%s' % (self.cid, self.csec))['authorization']

            response = self._http.urlopen('POST', self.idp_ep,
                                          body="grant_type=client_credentials",
                                          headers=headers,
                                          preload_content=True,
                                          )
            if response.status != 200:
                message = "Credential refresh failed, response: %s"
                raise CredentialRetrievalError(
                    provider=method,
                    error_msg=message % response.status,
                )

            creds = json.loads(response.data)

            query = {}
            query['Action'] = 'AssumeRoleWithClientGrants'
            query['Token'] = creds['access_token']
            query['DurationSeconds'] = creds['expires_in']
            query['Version'] = '2011-06-15'

            query_components = []
            for key in query:
                if query[key] is not None:
                    query_components.append("%s=%s" % (key, query[key]))

            query_string = '&'.join(query_components)
            sts_ep_url = self.sts_ep
            if query_string:
                sts_ep_url = self.sts_ep + '?' + query_string

            response = self._http.urlopen(
                'POST', sts_ep_url, preload_content=True)
            if response.status != 200:
                message = "Credential refresh failed, response: %s"
                raise CredentialRetrievalError(
                    provider=method,
                    error_msg=message % response.status,
                )

            return parse_grants_response(response.data)

        def parse_grants_response(data):
            """
            Parser for AssumeRoleWithClientGrants response

            :param data: Response data for AssumeRoleWithClientGrants request
            :return: dict
            """
            root = STSElement.fromstring(
                'AssumeRoleWithClientGrantsResponse', data)
            result = root.find('AssumeRoleWithClientGrantsResult')
            creds = result.find('Credentials')
            return dict(
                access_key=creds.get_child_text('AccessKeyId'),
                secret_key=creds.get_child_text('SecretAccessKey'),
                token=creds.get_child_text('SessionToken'),
                expiry_time=parse(creds.get_child_text('Expiration')).isoformat())

        return fetch_credentials
