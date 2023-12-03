#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Web Identity Service

Run this service along side your minio installation to provide a simple api that
allows users to log in using OIDC to get STS credentials.

See also web-identity-login.sh

Original source:
https://github.com/minio/minio/blob/3f81cd1b225309f147a75e3e773e495d6f178268/docs/sts/web-identity.md
License:
https://github.com/minio/minio/blob/3f81cd1b225309f147a75e3e773e495d6f178268/LICENSE
"""

import json
import logging
from typing import Optional, Union, cast
import urllib
from uuid import uuid4
import os
import sys
from datetime import timedelta
from urllib.parse import urlencode

from http import HTTPStatus
import asyncio
from dotenv import load_dotenv, dotenv_values
import tornado
from tornado.web import RequestHandler
from tornado.locks import Condition
from tornado.escape import utf8
from tornado.httpclient import AsyncHTTPClient, HTTPClientError
import xmltodict
from cachetools import TTLCache

# from xml import etree.ElementTree as etree


def eprint(msg):
    """Print to stderr"""
    print(msg, file=sys.stderr)


load_dotenv()
try:
    op_client_id = os.environ['WI_OP_CLIENT_ID']
    op_client_secret = os.environ['WI_OP_CLIENT_SECRET']
    op_oauth_url = os.environ['WI_OP_OAUTH_URL']
    endpoint_url = os.environ['WI_ENDPOINT_URL']
    callback_url = os.environ['WI_CALLBACK_URL']
    test_bucket_name = os.environ.get('WI_TEST_BUCKET')
    s3_region_name = os.environ.get('WI_S3_REGION_NAME', 'us-west-1')
    listen_port = int(os.environ.get('WI_LISTEN_PORT', '9002'))
    debug_mode = bool(int(os.environ.get('WI_DEBUG_MODE', '0')))
except KeyError as err:
    DOTENV_VALUES = '\n'.join([f'{k}={v}' for k, v in dotenv_values().items()])
    eprint(f'dotenv_values:\n{DOTENV_VALUES}\n')
    eprint(f'Environment variable not found: {err}')
    sys.exit(1)

# callback url specified when the application was defined
callback_uri = f'{callback_url}/oauth2/callback'
HTTPS_VERIFY = True

nonce_cache = TTLCache(maxsize=10000, ttl=60)
openid_cache = TTLCache(maxsize=1, ttl=60)

http_client = AsyncHTTPClient()


class HttpError(Exception):
    """Error class for raising http errors"""

    def __init__(self, *args: object) -> None:
        code, message = args
        self.code = code
        self.message = message
        super().__init__(*args)
    code: int
    message: str


async def get_openid_config():
    """Get the openid config (cached)"""
    config = openid_cache.get('config')
    if config is None:
        url = f'{op_oauth_url}/.well-known/openid-configuration'
        try:
            openid_config_response = await http_client.fetch(url)
            if openid_config_response.code != HTTPStatus.OK:
                raise HttpError(openid_config_response.code, f'Failed to get openid config from {url}')
            openid_config = json.loads(openid_config_response.body)
            if debug_mode:
                print(f'OpenID config: {openid_config}')
            config = {
                'token_url': openid_config['token_endpoint'],
                'authorize_url': openid_config['authorization_endpoint']
            }
            openid_cache['config'] = config
        except HTTPClientError as ex:
            raise HttpError(ex.code, f'Failed to get openid config from {url}') from ex
    return config


async def make_authorization_url():
    """Make an authorization url"""
    # Generate a random string for the state parameter
    # Save it for use later to prevent xsrf attacks

    openid_config = await get_openid_config()

    state = str(uuid4())
    params = {"client_id": op_client_id,
              "response_type": "code",
              "state": state,
              "redirect_uri": callback_uri,
              "scope": "openid"}

    url = openid_config['authorize_url'] + "?" + urllib.parse.urlencode(params)
    return url, state


# pylint: disable-next=W0223
class HomepageHandler(RequestHandler):
    """Test route that can be used to manually authenticate"""
    async def get(self):
        """Handle get"""
        try:
            url, _ = await make_authorization_url()
            # self.redirect calls finish() which doesn't allow us to send a message
            self.set_status(HTTPStatus.TEMPORARY_REDIRECT)
            self.set_header('Location', utf8(url))
            text = f'<a href="{url}">Authenticate with OIDC</a>'
            self.finish(text)
        except HttpError as ex:
            eprint(ex.message)
            self.send_error(ex.code, body=ex.message)

    def write_error(self, status_code, **kwargs):
        """Error writer override"""
        if 'body' in kwargs:
            self.write(f'{status_code}: {kwargs["body"]}')
        super().write_error(status_code, **kwargs)


# pylint: disable-next=W0223
class AuthUrlHandler(RequestHandler):
    """
    Auth initiator.
    Returns nonce which can be used to call auth_response/{nonce}
    and a url which can be used to initiate the OIDC authorization workflow
    """
    # use post since GET should be idempotent and this endpoint updates internal state
    # @app.route('/auth_url', methods=["POST"])
    async def post(self):
        """Handle post"""
        try:
            url, state = await make_authorization_url()

            nonce_cache[state] = (Condition(), None)

            self.finish({
                "nonce": state,
                "url": url
            })
        except HttpError as ex:
            eprint(ex.message)
            self.send_error(ex.code, json={
                'status_code': ex.code,
                'message': ex.message
            })

    def write_error(self, status_code, **kwargs):
        """Error writer override"""
        if 'json' in kwargs:
            self.finish(json.dumps(kwargs['json']))
        super().write_error(status_code, **kwargs)


# pylint: disable-next=W0223
class AuthResponseHandler(RequestHandler):
    """
    Auth Responder
    Post to this url will initiate a 'long poll' request which returns with the
    STS credentials when the user finishes the OIDC flow and credentials are returned
    to the callback url.
    """
    # use post since GET should be idempotent and this endpoint updates internal state
    # this is a long poll that will return when the callback happens
    async def post(self, **kwargs):
        "Handle post"
        nonce = kwargs.get('nonce')
        if nonce is None:
            self.send_error(HTTPStatus.BAD_REQUEST, message="Nonce was expected but not found")
            return
        condition: Optional[Condition]
        response: Optional[Union[dict, HttpError]]
        condition, response = nonce_cache.get(nonce, (None, None))
        if condition is None:
            self.send_error(HTTPStatus.UNAUTHORIZED, message="Unauthorized {nonce} not found")
            return
        if response is None:
            success = await condition.wait(timedelta(seconds=60))
            if not success:
                self.send_error(
                    HTTPStatus.GATEWAY_TIMEOUT,
                    message="Response timed out"
                )
                return
            _, response = nonce_cache.get(nonce, (None, None))
            nonce_cache.pop(nonce, None)
            if response is HttpError:
                self.send_error(response)
                return
            if response is None:
                self.send_error(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    message=f"Nonce {nonce} not found in cache when expecting response"
                )
                return
        self.finish(response)

    def write_error(self, status_code, **kwargs):
        """Error writer override"""
        if "message" in kwargs:
            self.write(kwargs["message"])
        super().write_error(status_code, **kwargs)


# pylint: disable-next=W0223
class CallbackHandler(RequestHandler):
    """
    Callback handler for OIDC workflow
    """
    # pylint: disable-next=too-many-locals
    async def get(self):
        """Handle get"""
        error = self.get_argument('error', '')
        if error:
            return "Error: " + error

        authorization_code = self.get_argument('code')
        state = self.get_argument('state')

        data = json.dumps({'grant_type': 'authorization_code',
                           'code': authorization_code, 'redirect_uri': callback_uri})
        openid_config = None
        try:
            openid_config = await get_openid_config()
        except HttpError as ex:
            self.send_error(ex.code)
            return
        id_token_response = None
        try:
            id_token_response = await http_client.fetch(
                openid_config['token_url'],
                method='POST',
                body=data,
                validate_cert=HTTPS_VERIFY,
                follow_redirects=False,
                auth_username=op_client_id,
                auth_password=op_client_secret
            )
        except HTTPClientError as ex:
            eprint(f'{ex.code}: {openid_config["token_url"]}: {ex.response.body}')
            self.send_error(ex.code, json={'response': str(ex.response.body), 'url': endpoint_url})
            return

        if debug_mode:
            print(f'body: {id_token_response.body}')

        # we can now use the id_token as much as we want to access protected
        # resources.
        tokens = json.loads(id_token_response.body)
        if id_token_response.code != HTTPStatus.OK:
            self.send_error(id_token_response.code, json=tokens)
            return
        params = {
            'Action': 'AssumeRoleWithWebIdentity',
            'WebIdentityToken': tokens['id_token'],
            'Version': '2011-06-15',
            # 'DurationSeconds': 86000,
        }
        assume_role_response = None
        try:
            url = f'{endpoint_url}?{urlencode(params)}'
            assume_role_response = await http_client.fetch(
                url,
                method='POST',
                body="",
                headers={'Accept': 'application/json'}
            )
        except HTTPClientError as ex:
            eprint(f'{ex.code}: {url}: {ex.response.body}')
            condition, _ = nonce_cache.get(state, (None, None))
            nonce_cache[state] = (condition, ex)
            condition.notify()
            self.send_error(ex.code, json={'response': str(ex.response.body), 'url': endpoint_url})
            return
        if debug_mode:
            print(f'assume {endpoint_url}')
            print(f's3 {assume_role_response.body}')
        response_dict = xmltodict.parse(assume_role_response.body)
        if assume_role_response.code != HTTPStatus.OK:
            self.send_error(assume_role_response.code, json=response_dict)
            return
        condition: Optional[Condition]
        response = response_dict[
            'AssumeRoleWithWebIdentityResponse']['AssumeRoleWithWebIdentityResult']
        json_response = json.dumps(response, indent=' ')
        if debug_mode:
            print(f'response_json {json_response}')
        self.test_bucket(response)

        condition, _ = nonce_cache.get(state, (None, None))
        if condition:
            nonce_cache[state] = (condition, response)
            condition.notify()
            body = '<br><br>'.join([
                'Success.',
                'You may now close this tab',
                f'Credentials will expire at {response["Credentials"]["Expiration"]}'
            ])
            self.finish(f'<!DOCTYPE html><html><body>{body}</body></html>')

        else:
            self.finish(response)

    def write_error(self, status_code, **kwargs):
        """Error writer override"""
        if 'json' in kwargs:
            self.write(json.dumps(kwargs['json']))
        super().write_error(status_code, **kwargs)

    def test_bucket(self, response: dict):
        """Test access to a bucket if that environment variable is set"""
        if test_bucket_name:
            # to run this code you need to install boto
            # pylint: disable-next=import-outside-toplevel, import-error
            import boto3
            # pylint: disable-next=import-outside-toplevel, import-error
            from botocore.client import Config
            boto3.set_stream_logger('boto3.resources', logging.DEBUG)
            s3_resource = boto3.resource('s3',
                                         endpoint_url=endpoint_url,
                                         aws_access_key_id=response['Credentials']['AccessKeyId'],
                                         aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                         aws_session_token=response['Credentials']['SessionToken'],
                                         config=Config(
                                             signature_version='s3v4'),
                                         region_name='us-east-1')

            bucket = s3_resource.Bucket(test_bucket_name)
            print(bucket)
            for obj in bucket.objects.all():
                print(obj)


async def main():
    """Start the webserver"""
    app = tornado.web.Application([
        (r'/', HomepageHandler),
        (r'/auth_url', AuthUrlHandler),
        (r'/auth_response/(?P<nonce>[-\w]{36})', AuthResponseHandler),
        (r'/oauth2/callback', CallbackHandler)
    ])
    app.listen(listen_port)
    shutdown_event = asyncio.Event()
    await shutdown_event.wait()

if __name__ == '__main__':
    # added in python 3.7
    if hasattr(asyncio, 'run'):
        asyncio.run(main())
    else:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
