/*
 * MinIO Cloud Storage (C) 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import SuperAgent from 'superagent-es6-promise';
import url from 'url'
import Moment from 'moment'

export default class JSONrpc {
  constructor(params) {
    this.endpoint = params.endpoint
    this.namespace = params.namespace
    this.version = '2.0';
    const parsedUrl = url.parse(this.endpoint)
    this.host = parsedUrl.hostname
    this.path = parsedUrl.path
    this.port = parsedUrl.port

    switch (parsedUrl.protocol) {
      case 'http:': {
        this.scheme = 'http'
        if (parsedUrl.port === 0) {
          this.port = 80
        }
        break
      }
      case 'https:': {
        this.scheme = 'https'
        if (parsedUrl.port === 0) {
          this.port = 443
        }
        break
      }
      default: {
        throw new Error('Unknown protocol: ' + parsedUrl.protocol)
      }
    }
  }
  // call('Get', {id: NN, params: [...]}, function() {})
  call(method, options, token) {
    if (!options) {
      options = {}
    }
    if (!options.id) {
      options.id = 1;
    }
    if (!options.params) {
      options.params = {};
    }
    const dataObj = {
      id: options.id,
      jsonrpc: this.version,
      params: options.params ? options.params : {},
      method: this.namespace ? this.namespace + '.' + method : method
    }
    let requestParams = {
      host: this.host,
      port: this.port,
      path: this.path,
      scheme: this.scheme,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-amz-date': Moment().utc().format('YYYYMMDDTHHmmss') + 'Z'
      }
    }

    if (token) {
      requestParams.headers.Authorization = 'Bearer ' + token
    }

    let req = SuperAgent.post(this.endpoint)
    for (let key in requestParams.headers) {
      req.set(key, requestParams.headers[key])
    }
    // req.set('Access-Control-Allow-Origin', 'http://localhost:8080')
    return req.send(JSON.stringify(dataObj)).then(res => res)
  }
}
