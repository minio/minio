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

import { minioBrowserPrefix } from './constants.js'

export const sortObjectsByName = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith('/'))
  let files = objects.filter(object => !object.name.endsWith('/'))
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
  if (order) {
    folders = folders.reverse()
    files = files.reverse()
  }
  return [...folders, ...files]
}

export const sortObjectsBySize = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith('/'))
  let files = objects.filter(object => !object.name.endsWith('/'))
  files = files.sort((a, b) => a.size - b.size)
  if (order)
    files = files.reverse()
  return [...folders, ...files]
}

export const sortObjectsByDate = (objects, order) => {
  let folders = objects.filter(object => object.name.endsWith('/'))
  let files = objects.filter(object => !object.name.endsWith('/'))
  files = files.sort((a, b) => new Date(a.lastModified).getTime() - new Date(b.lastModified).getTime())
  if (order)
    files = files.reverse()
  return [...folders, ...files]
}

export const pathSlice = (path) => {
  path = path.replace(minioBrowserPrefix, '')
  let prefix = ''
  let bucket = ''
  if (!path) return {
      bucket,
      prefix
  }
  let objectIndex = path.indexOf('/', 1)
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
  if (!prefix)
    prefix = ''
  return minioBrowserPrefix + '/' + bucket + '/' + prefix
}
