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
import web from "../web"
import classNames from "classnames"
import * as actionsBuckets from "../buckets/actions"
import * as uploadsActions from "../uploads/actions"
import { getPrefixWritable } from "../objects/selectors"
import ClickOutHandler from "react-onclickout"
import MakeBucketModal from "../buckets/MakeBucketModal"

export class MainActions extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      addNewActive: false,
      makeBucketActive: false
    }
  }

  onFileUpload(func, e) {
    e.preventDefault()
    let files = e.target.files
    let filesToUploadCount = files.length
    for (let i = 0; i < filesToUploadCount; i++) {
      func(files.item(i))
    }
    e.target.value = null
  }

  toggleAddNew() {
    this.setState({
      addNewActive: !this.state.addNewActive
    })
  }

  openMakeBucket() {
    this.setState({
      makeBucketActive: true
    })
  }

  closeMakeBucket() {
    if (this.state.makeBucketActive) {
      this.setState({
        makeBucketActive: false
      })
    } else {
      this.setState({
        addNewActive: false
      })
    }
  }

  render() {
    const { prefixWritable, uploadFile } = this.props
    const loggedIn = web.LoggedIn()

    if (loggedIn || prefixWritable) {
      return (
        <ClickOutHandler onClickOut={this.closeMakeBucket.bind(this)}>
          <div
            className={classNames({
              "add-new": true,
              "add-new--active": this.state.addNewActive,
              "add-new--bucket": this.state.makeBucketActive
            })}
          >
            <i
              className="add-new__toggle"
              onClick={this.toggleAddNew.bind(this)}
            />

            <label
              htmlFor="add-new-upload"
              className="add-new__item add-new__item--upload"
            >
              <input
                type="file"
                onChange={this.onFileUpload.bind(this, uploadFile)}
                id="add-new-upload"
                multiple={true}
              />
            </label>
            {loggedIn && (
              <div
                id="show-make-bucket"
                className="add-new__item add-new__item--bucket"
                onClick={
                  this.state.makeBucketActive
                    ? ""
                    : this.openMakeBucket.bind(this)
                }
              >
                <MakeBucketModal />
              </div>
            )}
            <div />
          </div>
        </ClickOutHandler>
      )
    } else {
      return <noscript />
    }
  }
}

const mapStateToProps = state => {
  return {
    prefixWritable: getPrefixWritable(state)
  }
}

const mapDispatchToProps = dispatch => {
  return {
    uploadFile: file => dispatch(uploadsActions.uploadFile(file)),
    showMakeBucketModal: () => dispatch(actionsBuckets.showMakeBucketModal())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(MainActions)
