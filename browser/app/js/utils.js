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

import { minioBrowserPrefix, SORT_ORDER_DESC } from "./constants.js"

export const sortObjectsByName = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith("/"))
  let files = objects.filter(object => !object.name.endsWith("/"))
  folders = folders.sort((a, b) => {
    if (a.name.toLowerCase() < b.name.toLowerCase()) return -1
    if (a.name.toLowerCase() > b.name.toLowerCase()) return 1
    return 0
  })
  files = files.sort((a, b) => {
    if (a.name.toLowerCase() < b.name.toLowerCase()) return -1
    if (a.name.toLowerCase() > b.name.toLowerCase()) return 1
    return 0
  })
  if (order === SORT_ORDER_DESC) {
    folders = folders.reverse()
    files = files.reverse()
  }
  return [...folders, ...files]
}

export const sortObjectsBySize = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith("/"))
  let files = objects.filter(object => !object.name.endsWith("/"))
  files = files.sort((a, b) => a.size - b.size)
  if (order === SORT_ORDER_DESC) files = files.reverse()
  return [...folders, ...files]
}

export const sortObjectsByDate = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith("/"))
  let files = objects.filter(object => !object.name.endsWith("/"))
  files = files.sort(
    (a, b) =>
      new Date(a.lastModified).getTime() - new Date(b.lastModified).getTime()
  )
  if (order === SORT_ORDER_DESC) files = files.reverse()
  return [...folders, ...files]
}

export const pathSlice = path => {
  path = path.replace(minioBrowserPrefix, "")
  let prefix = ""
  let bucket = ""
  if (!path)
    return {
      bucket,
      prefix
    }
  let objectIndex = path.indexOf("/", 1)
  if (objectIndex == -1) {
    bucket = path.slice(1)
    return {
      bucket,
      prefix
    }
  }
  bucket = path.slice(1, objectIndex)
  prefix = path.slice(objectIndex + 1)
  return {
    bucket,
    prefix
  }
}

export const pathJoin = (bucket, prefix) => {
  if (!prefix) prefix = ""
  return minioBrowserPrefix + "/" + bucket + "/" + prefix
}

export const getRandomAccessKey = () => {
  const alphaNumericTable = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  let arr = new Uint8Array(20)
  window.crypto.getRandomValues(arr)
  const random = Array.prototype.map.call(arr, v => {
    const i = v % alphaNumericTable.length
    return alphaNumericTable.charAt(i)
  })
  return random.join("")
}

export const getRandomSecretKey = () => {
  let arr = new Uint8Array(40)
  window.crypto.getRandomValues(arr)
  const binStr = Array.prototype.map
    .call(arr, v => {
      return String.fromCharCode(v)
    })
    .join("")
  const base64Str = btoa(binStr)
  return base64Str.replace(/\//g, "+").substr(0, 40)
}

export const getRandomString = length => {
  var text = ""
  var possible =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  for (var i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}
