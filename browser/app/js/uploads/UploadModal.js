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
