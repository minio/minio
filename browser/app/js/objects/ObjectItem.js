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
import { connect } from "react-redux"
import { getDataType } from "../mime"
import * as actions from "./actions"
import { getCheckedList } from "./selectors"
import classNames from "classnames"

export const ObjectItem = ({
  name,
  id,
  contentType,
  size,
  lastModified,
  checked,
  checkObject,
  uncheckObject,
  onClick
}) => {
  let nonFolderData = (
    <React.Fragment>
      <div className="objects__item objects__item--size">{size}</div>
      <div className="objects__item objects__item--modified">
        {lastModified}
      </div>
    </React.Fragment>
  )

  return (
    <label
      htmlFor={id}
      className={classNames({
        objects__row: true,
        "objects__row--checked": checked
      })}
    >
      <div className="objects__item">
        <div
          className={
            "objects__icon objects__icon--" + getDataType(name, contentType)
          }
        />

        <input
          id={id}
          className="objects__checkbox"
          type="checkbox"
          name={name}
          checked={checked}
          onChange={() => {
            checked ? uncheckObject(name) : checkObject(name)
          }}
        />
      </div>

      <div
        className="objects__item objects__item--name"
        onClick={e => {
          if (getDataType(name, contentType) === "folder") {
            e.preventDefault()
            onClick()
          }
        }}
      >
        {name}
      </div>

      {getDataType(name, contentType) != "folder" ? nonFolderData : ""}
    </label>
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
