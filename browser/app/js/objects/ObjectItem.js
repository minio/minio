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
import { connect } from "react-redux"
import humanize from "humanize"
import Moment from "moment"
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
  actionButtons,
  onClick
}) => {
  return (
    <div className={"fesl-row"} data-type={getDataType(name, contentType)}>
      <div className="fesl-item fesl-item-icon">
        <div className="fi-select">
          <input
            type="checkbox"
            name={name}
            checked={checked}
            onChange={() => {
              checked ? uncheckObject(name) : checkObject(name)
            }}
          />
          <i className="fis-icon" />
          <i className="fis-helper" />
        </div>
      </div>
      <div className="fesl-item fesl-item-name">
        <a
          href={getDataType(name, contentType) === "folder" ? name : "#"}
          onClick={e => {
            e.preventDefault()
            // onclick function is passed only when we have a prefix
            if (onClick) {
              onClick()
            } else {
              checked ? uncheckObject(name) : checkObject(name)
            }
          }}
        >
          {name}
        </a>
      </div>
      <div className="fesl-item fesl-item-size">{size}</div>
      <div className="fesl-item fesl-item-modified">{lastModified}</div>
      <div className="fesl-item fesl-item-actions">{actionButtons}</div>
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
