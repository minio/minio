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

import * as actionsObjects from "./actions"
import { SORT_ORDER_ASC } from "../constants"

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
    filter: "",
    listLoading: false,
    sortBy: "",
    sortOrder: SORT_ORDER_ASC,
    currentPrefix: "",
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
        list: action.objects
      }
    case actionsObjects.RESET_LIST:
      return {
        ...state,
        list: []
      }
    case actionsObjects.SET_FILTER:
      return {
        ...state,
        filter: action.filter
      }
    case actionsObjects.SET_LIST_LOADING:
      return {
        ...state,
        listLoading: action.listLoading
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
        currentPrefix: action.prefix
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
          url: action.url,
          showExpiryDate: action.showExpiryDate
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
