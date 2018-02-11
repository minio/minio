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
import humanize from "humanize"
import * as actionsCommon from "./actions"

export class StorageInfo extends React.Component {
  componentWillMount() {
    const { fetchStorageInfo } = this.props
    fetchStorageInfo()
  }
  render() {
    const { total, free } = this.props.storageInfo
    const used = total - free
    const usedPercent = used / total * 100 + "%"
    const freePercent = free * 100 / total
    return (
      <div className="feh-usage">
        <div className="fehu-chart">
          <div style={{ width: usedPercent }} />
        </div>
        <ul>
          <li>
            <span>Used: </span>
            {humanize.filesize(total - free)}
          </li>
          <li className="pull-right">
            <span>Free: </span>
            {humanize.filesize(total - used)}
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

export default connect(mapStateToProps, mapDispatchToProps)(StorageInfo)
