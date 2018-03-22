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
import * as actionsBuckets from "./actions"
import { getCurrentBucket } from "./selectors"
import { Dropdown } from "react-bootstrap"
import { MenuItem } from "react-bootstrap"

export class BucketDropdown extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      showBucketDropdown: false
    }
  }

  toggleDropdown() {
    if (this.state.showBucketDropdown) {
      this.setState({
        showBucketDropdown: false
      })
    } else {
      this.setState({
        showBucketDropdown: true
      })
    }
  }

  render() {
    const { bucket, showBucketPolicy, deleteBucket, currentBucket } = this.props
    return (
      <Dropdown
        pullRight
        open={this.state.showBucketDropdown}
        onToggle={this.toggleDropdown.bind(this)}
        className="buckets__item__actions"
        id="bucket-dropdown"
      >
        <Dropdown.Toggle noCaret className="dropdown-toggle--icon">
          <i className="zmdi zmdi-more-vert" />
        </Dropdown.Toggle>
        <Dropdown.Menu>
          <MenuItem
            onClick={e => {
              e.stopPropagation()
              showBucketPolicy()
            }}
          >
            {" "}
            Edit Policy
          </MenuItem>
          <MenuItem
            onClick={e => {
              e.stopPropagation()
              deleteBucket(bucket)
            }}
          >
            {" "}
            Delete Bucket
          </MenuItem>
        </Dropdown.Menu>
      </Dropdown>
    )
  }
}

const mapDispatchToProps = dispatch => {
  return {
    deleteBucket: bucket => dispatch(actionsBuckets.deleteBucket(bucket)),
    showBucketPolicy: () => dispatch(actionsBuckets.showBucketPolicy())
  }
}

export default connect(state => state, mapDispatchToProps)(BucketDropdown)
