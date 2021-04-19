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
import * as actionsCommon from "./actions"

export class StorageInfo extends React.Component {
  componentWillMount() {
    const { fetchStorageInfo } = this.props
    fetchStorageInfo()
  }
  render() {
    const { used } = this.props.storageInfo
    if (!used || used == 0) {
      return <noscript />
    }

    return (
      <div className="feh-used">
        <div className="fehu-chart">
          <div style={{ width: 0 }} />
        </div>
        <ul>
          <li>
            <span>Used: </span>
            {humanize.filesize(used)}
          </li>
        </ul>
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    storageInfo: state.browser.storageInfo
  }
}

const mapDispatchToProps = dispatch => {
  return {
    fetchStorageInfo: () => dispatch(actionsCommon.fetchStorageInfo())
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(StorageInfo)
