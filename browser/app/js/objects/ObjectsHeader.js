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

import React from "react"
import classNames from "classnames"
import { connect } from "react-redux"
import * as actionsObjects from "./actions"

export const ObjectsHeader = ({
  sortNameOrder,
  sortSizeOrder,
  sortLastModifiedOrder,
  sortObjects
}) => (
  <div className="feb-container">
    <header className="fesl-row" data-type="folder">
      <div className="fesl-item fesl-item-icon" />
      <div
        className="fesl-item fesl-item-name"
        id="sort-by-name"
        onClick={() => sortObjects("name")}
        data-sort="name"
      >
        Name
        <i
          className={classNames({
            "fesli-sort": true,
            fa: true,
            "fa-sort-alpha-desc": sortNameOrder,
            "fa-sort-alpha-asc": !sortNameOrder
          })}
        />
      </div>
      <div
        className="fesl-item fesl-item-size"
        id="sort-by-size"
        onClick={() => sortObjects("size")}
        data-sort="size"
      >
        Size
        <i
          className={classNames({
            "fesli-sort": true,
            fa: true,
            "fa-sort-amount-desc": sortSizeOrder,
            "fa-sort-amount-asc": !sortSizeOrder
          })}
        />
      </div>
      <div
        className="fesl-item fesl-item-modified"
        id="sort-by-last-modified"
        onClick={() => sortObjects("last-modified")}
        data-sort="last-modified"
      >
        Last Modified
        <i
          className={classNames({
            "fesli-sort": true,
            fa: true,
            "fa-sort-numeric-desc": sortLastModifiedOrder,
            "fa-sort-numeric-asc": !sortLastModifiedOrder
          })}
        />
      </div>
      <div className="fesl-item fesl-item-actions" />
    </header>
  </div>
)

const mapStateToProps = state => {
  return {
    sortNameOrder: state.objects.sortBy == "name" && state.objects.sortOrder,
    sortSizeOrder: state.objects.sortBy == "size" && state.objects.sortOrder,
    sortLastModifiedOrder:
      state.objects.sortBy == "last-modified" && state.objects.sortOrder
  }
}

const mapDispatchToProps = dispatch => {
  return {
    sortObjects: sortBy => dispatch(actionsObjects.sortObjects(sortBy))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ObjectsHeader)
