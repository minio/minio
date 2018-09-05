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
  <div className="objects__header">
    <div className="objects__row objects__row--header">
      <div
        className="objects__item objects__item--name"
        id="sort-by-name"
        onClick={() => sortObjects("name")}
      >
        Name
        <div
          className={classNames({
            objects__sort: true,
            "objects__sort--asc": !sortNameOrder,
            "objects__sort--desc": sortNameOrder
          })}
        >
          <i />
          <i />
          <i />
        </div>
      </div>

      <div
        className={classNames({
          "objects__item objects__item--size": true,
          "objects__item--sort-asc": !sortSizeOrder,
          "objects__item--sort-desc": sortSizeOrder
        })}
        id="sort-by-size"
        onClick={() => sortObjects("size")}
      >
        Size
        <div
          className={classNames({
            objects__sort: true,
            "objects__sort--asc": !sortSizeOrder,
            "objects__sort--desc": sortSizeOrder
          })}
        >
          <i />
          <i />
          <i />
        </div>
      </div>

      <div
        className={classNames({
          "objects__item objects__item--modified": true,
          "objects__item--sort-asc": !sortLastModifiedOrder,
          "objects__item--sort-desc": sortLastModifiedOrder
        })}
        id="sort-by-last-modified"
        onClick={() => sortObjects("last-modified")}
      >
        Last Modified
        <div
          className={classNames({
            objects__sort: true,
            "objects__sort--asc": !sortLastModifiedOrder,
            "objects__sort--desc": sortLastModifiedOrder
          })}
        >
          <i />
          <i />
          <i />
        </div>
      </div>
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
