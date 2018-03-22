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
  const uploadTooltip = <Tooltip id="tooltip-upload-file">Upload file</Tooltip>
  const makeBucketTooltip = (
    <Tooltip id="tooltip-create-bucket">Create bucket</Tooltip>
  )
  const onFileUpload = e => {
    e.preventDefault()
    uploadFile(e.target.files[0])
    e.target.value = null
  }

  const loggedIn = web.LoggedIn()

  if (loggedIn || prefixWritable) {
    return (
      <Dropdown
        dropup
        pullRight
        className="create"
        id="main-actions"
        componentClass="div"
      >
        <Dropdown.Toggle noCaret className="create__toggle" useAnchor={true}>
          <i className="zmdi zmdi-plus" />
          <i className="zmdi zmdi-close" />
        </Dropdown.Toggle>
        <Dropdown.Menu>
          <OverlayTrigger placement="left" overlay={uploadTooltip}>
            <a href="#" className="create__btn create__btn--upload">
              <input
                type="file"
                onChange={onFileUpload}
                style={{ display: "none" }}
                id="file-input"
              />
              <label htmlFor="file-input">
                <i className="zmdi zmdi-upload" />
              </label>
            </a>
          </OverlayTrigger>
          {loggedIn && (
            <OverlayTrigger placement="left" overlay={makeBucketTooltip}>
              <a
                href="#"
                id="show-make-bucket"
                className="create__btn create__btn--bucket"
                onClick={e => {
                  e.preventDefault()
                  showMakeBucketModal()
                }}
              />
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
