/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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

import React from "react"
import classNames from "classnames"
import { connect } from "react-redux"
import * as actionsObjects from "./actions"
import {
  SORT_BY_NAME,
  SORT_BY_SIZE,
  SORT_BY_LAST_MODIFIED,
  SORT_ORDER_DESC,
  SORT_ORDER_ASC
} from "../constants"

export const ObjectsHeader = ({
  sortedByName,
  sortedBySize,
  sortedByLastModified,
  sortOrder,
  sortObjects
}) => (
  <div className="feb-container">
    <header className="fesl-row" data-type="folder">
      <div className="fesl-item fesl-item-icon" />
      <div
        className="fesl-item fesl-item-name"
        id="sort-by-name"
        onClick={() => sortObjects(SORT_BY_NAME)}
        data-sort="name"
      >
        Name
        <i
          className={classNames({
            "fesli-sort": true,
            "fesli-sort--active": sortedByName,
            fa: true,
            "fa-sort-alpha-desc": sortedByName && sortOrder === SORT_ORDER_DESC,
            "fa-sort-alpha-asc": sortedByName && sortOrder === SORT_ORDER_ASC
          })}
        />
      </div>
      <div
        className="fesl-item fesl-item-size"
        id="sort-by-size"
        onClick={() => sortObjects(SORT_BY_SIZE)}
        data-sort="size"
      >
        Size
        <i
          className={classNames({
            "fesli-sort": true,
            "fesli-sort--active": sortedBySize,
            fa: true,
            "fa-sort-amount-desc":
              sortedBySize && sortOrder === SORT_ORDER_DESC,
            "fa-sort-amount-asc": sortedBySize && sortOrder === SORT_ORDER_ASC
          })}
        />
      </div>
      <div
        className="fesl-item fesl-item-modified"
        id="sort-by-last-modified"
        onClick={() => sortObjects(SORT_BY_LAST_MODIFIED)}
        data-sort="last-modified"
      >
        Last Modified
        <i
          className={classNames({
            "fesli-sort": true,
            "fesli-sort--active": sortedByLastModified,
            fa: true,
            "fa-sort-numeric-desc":
              sortedByLastModified && sortOrder === SORT_ORDER_DESC,
            "fa-sort-numeric-asc":
              sortedByLastModified && sortOrder === SORT_ORDER_ASC
          })}
        />
      </div>
      <div className="fesl-item fesl-item-actions" />
    </header>
  </div>
)

const mapStateToProps = state => {
  return {
    sortedByName: state.objects.sortBy == SORT_BY_NAME,
    sortedBySize: state.objects.sortBy == SORT_BY_SIZE,
    sortedByLastModified: state.objects.sortBy == SORT_BY_LAST_MODIFIED,
    sortOrder: state.objects.sortOrder
  }
}

const mapDispatchToProps = dispatch => {
  return {
    sortObjects: sortBy => dispatch(actionsObjects.sortObjects(sortBy))
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(ObjectsHeader)
