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
import BucketDropdown from "./BucketDropdown"
import ClickOutHandler from "react-onclickout"
import * as actionsCommon from "../browser/actions"
import { connect } from "react-redux"

export class Bucket extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      bucketDropdownActive: false,
      bucketDropdownEnters: false
    }
    this.hideBucketDropdown = this.hideBucketDropdown.bind(this)
  }

  showBucketDropdown() {
    this.setState({
      bucketDropdownActive: true
    })

    setTimeout(() => {
      this.setState({
        bucketDropdownEnters: true
      })
    }, 1)
  }

  hideBucketDropdown() {
    this.setState({
      bucketDropdownEnters: false
    })

    setTimeout(() => {
      this.setState({
        bucketDropdownActive: false
      })
    }, 300)
  }

  hideBucketDropdownOnEscape(e) {
    if (e.keyCode === 27) {
      this.hideBucketDropdown()
    }
  }

  render() {
    const { bucket, isActive, selectBucket, closeSidebar } = this.props

    return (
      <div
        className={classNames({
          buckets__item: true,
          "buckets__item--active": isActive,
          "buckets__item--enters": this.state.bucketDropdownEnters
        })}
        onClick={e => {
          e.preventDefault()
          setTimeout(() => {
            selectBucket(bucket)
          })

          if (e.target.className != "buckets__toggle") {
            setTimeout(() => {
              closeSidebar()
            }, 300)
          }
        }}
      >
        <span>{bucket}</span>
        <div className="buckets__dropdown">
          <i
            tabIndex="0"
            className="buckets__toggle"
            onClick={
              this.state.bucketDropdownActive
                ? this.hideBucketDropdown
                : this.showBucketDropdown.bind(this)
            }
            onKeyDown={this.hideBucketDropdownOnEscape.bind(this)}
          />

          {this.state.bucketDropdownActive && (
            <ClickOutHandler onClickOut={this.hideBucketDropdown}>
              <BucketDropdown bucket={bucket} />
            </ClickOutHandler>
          )}
        </div>
      </div>
    )
  }
}

const mapDispatchToProps = dispatch => {
  return {
    closeSidebar: () => dispatch(actionsCommon.closeSidebar())
  }
}

export default connect(undefined, mapDispatchToProps)(Bucket)
