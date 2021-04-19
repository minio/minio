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

import JSONrpc from './jsonrpc'
import { minioBrowserPrefix } from './constants.js'
import Moment from 'moment'
import storage from 'local-storage-fallback'

class Web {
  constructor(endpoint) {
    const namespace = 'web'
    this.JSONrpc = new JSONrpc({
      endpoint,
      namespace
    })
  }
  makeCall(method, options) {
    return this.JSONrpc.call(method, {
      params: options
    }, storage.getItem('token'))
      .catch(err => {
        if (err.status === 401) {
          storage.removeItem('token')
          location.reload()
          throw new Error('Please re-login.')
        }
        if (err.status)
          throw new Error(`Server returned error [${err.status}]`)
        throw new Error('MinIO server is unreachable')
      })
      .then(res => {
        let json = JSON.parse(res.text)
        let result = json.result
        let error = json.error
        if (error) {
          throw new Error(error.message)
        }
        if (!Moment(result.uiVersion).isValid()) {
          throw new Error("Invalid UI version in the JSON-RPC response")
        }
        if (result.uiVersion !== currentUiVersion
          && currentUiVersion !== 'MINIO_UI_VERSION') {
          storage.setItem('newlyUpdated', true)
          location.reload()
        }
        return result
      })
  }
  LoggedIn() {
    return !!storage.getItem('token')
  }
  Login(args) {
    return this.makeCall('Login', args)
      .then(res => {
        storage.setItem('token', `${res.token}`)
        return res
      })
  }
  Logout() {
    storage.removeItem('token')
  }
  GetToken() {
    return storage.getItem('token')
  }
  GetDiscoveryDoc() {
    return this.makeCall("GetDiscoveryDoc")
  }
  LoginSTS(args) {
    return this.makeCall('LoginSTS', args)
      .then(res => {
        storage.setItem('token', `${res.token}`)
        return res
      })
  }
  ServerInfo() {
    return this.makeCall('ServerInfo')
  }
  StorageInfo() {
    return this.makeCall('StorageInfo')
  }
  ListBuckets() {
    return this.makeCall('ListBuckets')
  }
  MakeBucket(args) {
    return this.makeCall('MakeBucket', args)
  }
  DeleteBucket(args) {
    return this.makeCall('DeleteBucket', args)
  }
  ListObjects(args) {
    return this.makeCall('ListObjects', args)
  }
  PresignedGet(args) {
    return this.makeCall('PresignedGet', args)
  }
  PutObjectURL(args) {
    return this.makeCall('PutObjectURL', args)
  }
  RemoveObject(args) {
    return this.makeCall('RemoveObject', args)
  }
  SetAuth(args) {
    return this.makeCall('SetAuth', args)
      .then(res => {
        storage.setItem('token', `${res.token}`)
        return res
      })
  }
  CreateURLToken() {
    return this.makeCall('CreateURLToken')
  }
  GetBucketPolicy(args) {
    return this.makeCall('GetBucketPolicy', args)
  }
  SetBucketPolicy(args) {
    return this.makeCall('SetBucketPolicy', args)
  }
  ListAllBucketPolicies(args) {
    return this.makeCall('ListAllBucketPolicies', args)
  }
}

const web = new Web(`${window.location.protocol}//${window.location.host}${minioBrowserPrefix}/webrpc`);

export default web;
