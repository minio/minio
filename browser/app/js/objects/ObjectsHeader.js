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
  <div className="objects__row objects__header hidden-xs">
    <div
      className="objects__column objects__column--name"
      id="sort-by-name"
      onClick={() => sortObjects("name")}
      data-sort="name"
    >
      Name
      <i
        className={classNames({
          objects__sort: true,
          zmdi: true,
          "zmdi-sort-desc": sortNameOrder,
          "zmdi-sort-asc": !sortNameOrder
        })}
      />
    </div>
    <div
      className="objects__column objects__column--size"
      id="sort-by-size"
      onClick={() => sortObjects("size")}
      data-sort="size"
    >
      Size
      <i
        className={classNames({
          objects__sort: true,
          zmdi: true,
          "zmdi-sort-amount-desc": sortSizeOrder,
          "zmdi-sort-amount-asc": !sortSizeOrder
        })}
      />
    </div>
    <div
      className="objects__column objects__column--date"
      id="sort-by-last-modified"
      onClick={() => sortObjects("last-modified")}
      data-sort="last-modified"
    >
      Last Modified
      <i
        className={classNames({
          objects__sort: true,
          zmdi: true,
          "zmdi-sort-amount-desc": sortLastModifiedOrder,
          "zmdi-sort-amount-asc": !sortLastModifiedOrder
        })}
      />
    </div>
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
