#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import urllib
from uuid import uuid4

import boto3
import requests
from botocore.client import Config
from flask import Flask, request, session

boto3.set_stream_logger('boto3.resources', logging.DEBUG)

authorize_url = "http://localhost:8080/auth/realms/minio/protocol/openid-connect/auth"
token_url = "http://localhost:8080/auth/realms/minio/protocol/openid-connect/token"

# callback url specified when the application was defined
callback_uri = "http://localhost:8000/oauth2/callback"

# keycloak id and secret
client_id = os.environ.get('CLIENT_ID', 'account')
client_secret = os.environ.get('CLIENT_SECRET', '')

sts_client = boto3.client(
    'sts',
    region_name='us-east-1',
    use_ssl=False,
    endpoint_url='http://localhost:9000',
)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-key-only-for-testing')


@app.route('/')
def homepage():
    text = '<a href="%s">Authenticate with keycloak</a>'
    return text % make_authorization_url()


def make_authorization_url():
    # Generate a random string for the state parameter
    # Save it for use later to prevent xsrf attacks

    state = str(uuid4())
    session['oauth_state'] = state
    params = {"client_id": client_id,
              "response_type": "code",
              "state": state,
              "redirect_uri": callback_uri,
              "scope": "openid"}

    url = authorize_url + "?" + urllib.parse.urlencode(params)
    return url


@app.route('/oauth2/callback')
def callback():
    error = request.args.get('error', '')
    if error:
        return "Error: " + error

    # Validate the state parameter to prevent CSRF attacks
    state = request.args.get('state', '')
    session_state = session.get('oauth_state', '')
    
    if not state or state != session_state:
        return "Error: Invalid state parameter - CSRF attack detected", 403
    
    # Clear the state from session after validation
    if 'oauth_state' in session:
        del session['oauth_state']

    authorization_code = request.args.get('code')

    data = {'grant_type': 'authorization_code',
            'code': authorization_code, 'redirect_uri': callback_uri}
    id_token_response = requests.post(
        token_url, data=data, verify=True,
        allow_redirects=False, auth=(client_id, client_secret))

    print('body: ' + id_token_response.text)

    # we can now use the id_token as much as we want to access protected resources.
    tokens = json.loads(id_token_response.text)
    id_token = tokens['id_token']

    response = sts_client.assume_role_with_web_identity(
        RoleArn='arn:aws:iam::123456789012:user/svc-internal-api',
        RoleSessionName='test',
        WebIdentityToken=id_token,
        DurationSeconds=3600
    )

    s3_resource = boto3.resource('s3',
                                 endpoint_url='http://localhost:9000',
                                 aws_access_key_id=response['Credentials']['AccessKeyId'],
                                 aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                 aws_session_token=response['Credentials']['SessionToken'],
                                 config=Config(signature_version='s3v4'),
                                 region_name='us-east-1')

    bucket = s3_resource.Bucket('testbucket')

    for obj in bucket.objects.all():
        print(obj)

    return "success"


if __name__ == '__main__':
    app.run(debug=True, port=8000)
