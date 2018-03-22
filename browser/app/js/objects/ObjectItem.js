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
import { getDataType } from "../mime"
import * as actions from "./actions"
import { getCheckedList } from "./selectors"

export const ObjectItem = ({
  name,
  contentType,
  size,
  lastModified,
  checked,
  checkObject,
  uncheckObject,
  onClick
}) => {
  return (
    <div
      className={classNames({
        objects__row: true,
        "objects__row--directory": getDataType(name, contentType) == "folder"
      })}
    >
      <div
        className="objects__column objects__column--select"
        data-object-type={getDataType(name, contentType)}
      >
        <div className="objects__select">
          <input
            type="checkbox"
            name={name}
            checked={checked}
            onChange={() => {
              checked ? uncheckObject(name) : checkObject(name)
            }}
          />
          <i />
        </div>
      </div>
      <div className="objects__column objects__column--name">
        <a
          href="#"
          onClick={e => {
            e.preventDefault()
            onClick()
          }}
        >
          {name}
        </a>
      </div>
      <div className="objects__column objects__column--size">{size}</div>
      <div className="objects__column objects__column--date">
        {lastModified}
      </div>
    </div>
  )
}

const mapStateToProps = (state, ownProps) => {
  return {
    checked: getCheckedList(state).indexOf(ownProps.name) >= 0
  }
}

const mapDispatchToProps = dispatch => {
  return {
    checkObject: name => dispatch(actions.checkObject(name)),
    uncheckObject: name => dispatch(actions.uncheckObject(name))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ObjectItem)
