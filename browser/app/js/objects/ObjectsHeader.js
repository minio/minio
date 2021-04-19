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
            fas: true,
            "fa-sort-alpha-down-alt": sortedByName && sortOrder === SORT_ORDER_DESC,
            "fa-sort-alpha-down": sortedByName && sortOrder === SORT_ORDER_ASC
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
            fas: true,
            "fa-sort-amount-down":
              sortedBySize && sortOrder === SORT_ORDER_DESC,
            "fa-sort-amount-down-alt": sortedBySize && sortOrder === SORT_ORDER_ASC
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
            fas: true,
            "fa-sort-numeric-down-alt":
              sortedByLastModified && sortOrder === SORT_ORDER_DESC,
            "fa-sort-numeric-down":
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
