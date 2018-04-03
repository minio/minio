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
import { Dropdown, OverlayTrigger, Tooltip } from "react-bootstrap"
import web from "../web"
import * as actionsBuckets from "../buckets/actions"
import * as uploadsActions from "../uploads/actions"
import { getPrefixWritable } from "../objects/selectors"

export const MainActions = ({
  prefixWritable,
  uploadFile,
  showMakeBucketModal
}) => {
  const uploadTooltip = <Tooltip id="tt-upload-file">Upload file</Tooltip>
  const makeBucketTooltip = (
    <Tooltip id="tt-create-bucket">Create bucket</Tooltip>
  )
  const onFileUpload = e => {
    e.preventDefault()
    let files = e.target.files
    let filesToUploadCount = files.length
    for (let i = 0; i < filesToUploadCount; i++) {
      uploadFile(files.item(i))
    }
    e.target.value = null
  }

  const loggedIn = web.LoggedIn()

  if (loggedIn || prefixWritable) {
    return (
      <Dropdown dropup className="feb-actions" id="fe-action-toggle">
        <Dropdown.Toggle noCaret className="feba-toggle">
          <span>
            <i className="fa fa-plus" />
          </span>
        </Dropdown.Toggle>
        <Dropdown.Menu>
          <OverlayTrigger placement="left" overlay={uploadTooltip}>
            <a href="#" className="feba-btn feba-upload">
              <input
                type="file"
                onChange={onFileUpload}
                style={{ display: "none" }}
                id="file-input"
                multiple={true}
              />
              <label htmlFor="file-input">
                {" "}
                <i className="fa fa-cloud-upload" />{" "}
              </label>
            </a>
          </OverlayTrigger>
          {loggedIn && (
            <OverlayTrigger placement="left" overlay={makeBucketTooltip}>
              <a
                href="#"
                id="show-make-bucket"
                className="feba-btn feba-bucket"
                onClick={e => {
                  e.preventDefault()
                  showMakeBucketModal()
                }}
              >
                <i className="fa fa-hdd-o" />
              </a>
            </OverlayTrigger>
          )}
        </Dropdown.Menu>
      </Dropdown>
    )
  } else {
    return <noscript />
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
