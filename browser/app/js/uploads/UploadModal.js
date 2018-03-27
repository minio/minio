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
import humanize from "humanize"
import classNames from "classnames"
import { connect } from "react-redux"

import { ProgressBar } from "react-bootstrap"
import AbortConfirmModal from "./AbortConfirmModal"
import * as uploadsActions from "./actions"

export class UploadModal extends React.Component {
  render() {
    const { uploads, showAbort, showAbortModal } = this.props
    if (showAbort) {
      return <AbortConfirmModal />
    }

    // If we don't have any files uploading, don't show anything.
    let numberUploading = Object.keys(uploads).length
    if (numberUploading == 0) return <noscript />

    let totalLoaded = 0
    let totalSize = 0

    // Iterate over each upload, adding together the total size and that
    // which has been uploaded.
    for (var slug in uploads) {
      let upload = uploads[slug]
      totalLoaded += upload.loaded
      totalSize += upload.size
    }

    let percent = totalLoaded / totalSize * 100

    // If more than one: "Uploading files (5)..."
    // If only one: "Uploading myfile.txt..."
    let text =
      "Uploading " +
      (numberUploading == 1
        ? `'${uploads[Object.keys(uploads)[0]].name}'`
        : `files (${numberUploading})`) +
      "..."

    return (
      <div className="alert alert-info progress animated fadeInUp ">
        <button type="button" className="close" onClick={showAbortModal}>
          <span>×</span>
        </button>
        <div className="text-center">
          <small>{text}</small>
        </div>
        <ProgressBar now={percent} />
        <div className="text-center">
          <small>
            {humanize.filesize(totalLoaded)} ({percent.toFixed(2)} %)
          </small>
        </div>
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    uploads: state.uploads.files,
    showAbort: state.uploads.showAbortModal
  }
}

const mapDispatchToProps = dispatch => {
  return {
    showAbortModal: () => dispatch(uploadsActions.showAbortModal())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(UploadModal)
