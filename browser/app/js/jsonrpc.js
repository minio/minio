/*
 * Copyright (c) 2015-2021 MinIO, Inc.
 *
 * This file is part of MinIO Object Storage stack
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
