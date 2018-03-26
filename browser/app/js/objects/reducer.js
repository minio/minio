/*
 * Minio Cloud Storage (C) 2018 Minio, Inc.
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

import * as actionsObjects from "./actions"

const removeObject = (list, objectToRemove, lookup) => {
  const idx = list.findIndex(object => lookup(object) === objectToRemove)
  if (idx == -1) {
    return list
  }
  return [...list.slice(0, idx), ...list.slice(idx + 1)]
}

export default (
  state = {
    list: [],
    sortBy: "",
    sortOrder: false,
    currentPrefix: "",
    marker: "",
    isTruncated: false,
    prefixWritable: false,
    shareObject: {
      show: false,
      object: "",
      url: ""
    },
    checkedList: []
  },
  action
) => {
  switch (action.type) {
    case actionsObjects.SET_LIST:
      return {
        ...state,
        list: action.objects,
        marker: action.marker,
        isTruncated: action.isTruncated
      }
    case actionsObjects.APPEND_LIST:
      return {
        ...state,
        list: [...state.list, ...action.objects],
        marker: action.marker,
        isTruncated: action.isTruncated
      }
    case actionsObjects.REMOVE:
      return {
        ...state,
        list: removeObject(state.list, action.object, object => object.name)
      }
    case actionsObjects.SET_SORT_BY:
      return {
        ...state,
        sortBy: action.sortBy
      }
    case actionsObjects.SET_SORT_ORDER:
      return {
        ...state,
        sortOrder: action.sortOrder
      }
    case actionsObjects.SET_CURRENT_PREFIX:
      return {
        ...state,
        currentPrefix: action.prefix,
        marker: "",
        isTruncated: false
      }
    case actionsObjects.SET_PREFIX_WRITABLE:
      return {
        ...state,
        prefixWritable: action.prefixWritable
      }
    case actionsObjects.SET_SHARE_OBJECT:
      return {
        ...state,
        shareObject: {
          show: action.show,
          object: action.object,
          url: action.url
        }
      }
    case actionsObjects.CHECKED_LIST_ADD:
      return {
        ...state,
        checkedList: [...state.checkedList, action.object]
      }
    case actionsObjects.CHECKED_LIST_REMOVE:
      return {
        ...state,
        checkedList: removeObject(
          state.checkedList,
          action.object,
          object => object
        )
      }
    case actionsObjects.CHECKED_LIST_RESET:
      return {
        ...state,
        checkedList: []
      }
    default:
      return state
  }
}
