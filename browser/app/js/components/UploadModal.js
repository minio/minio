/*
 * Minio Browser (C) 2016 Minio, Inc.
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

import React from 'react'
import humanize from 'humanize'
import classNames from 'classnames'
import connect from 'react-redux/lib/components/connect'

import ProgressBar from 'react-bootstrap/lib/ProgressBar'
import ConfirmModal from './ConfirmModal'

import * as actions from '../actions'

// UploadModal is a modal that handles multiple file uploads.
// During the upload, it displays a progress bar, and can transform into an
// abort modal if the user decides to abort the uploads.
class UploadModal extends React.Component {

  // Abort all the current uploads.
  abortUploads(e) {
    e.preventDefault()
    const {dispatch, uploads} = this.props

    for (var slug in uploads) {
      let upload = uploads[slug]
      upload.xhr.abort()
      dispatch(actions.stopUpload({
        slug
      }))
    }

    this.hideAbort(e)
  }

  // Show the abort modal instead of the progress modal.
  showAbort(e) {
    e.preventDefault()
    const {dispatch} = this.props

    dispatch(actions.setShowAbortModal(true))
  }

  // Show the progress modal instead of the abort modal.
  hideAbort(e) {
    e.preventDefault()
    const {dispatch} = this.props

    dispatch(actions.setShowAbortModal(false))
  }

  render() {
    const {uploads, showAbortModal} = this.props

    // Show the abort modal.
    if (showAbortModal) {
      let baseClass = classNames({
        'abort-upload': true
      })
      let okIcon = classNames({
        'fa': true,
        'fa-times': true
      })
      let cancelIcon = classNames({
        'fa': true,
        'fa-cloud-upload': true
      })

      return (
        <ConfirmModal show={ true }
          baseClass={ baseClass }
          text='Abort uploads in progress?'
          icon='fa fa-info-circle mci-amber'
          sub='This cannot be undone!'
          okText='Abort'
          okIcon={ okIcon }
          cancelText='Upload'
          cancelIcon={ cancelIcon }
          okHandler={ this.abortUploads.bind(this) }
          cancelHandler={ this.hideAbort.bind(this) }>
        </ConfirmModal>
      )
    }

    // If we don't have any files uploading, don't show anything.
    let numberUploading = Object.keys(uploads).length
    if (numberUploading == 0)
      return ( <noscript></noscript> )

    let totalLoaded = 0
    let totalSize = 0

    // Iterate over each upload, adding together the total size and that
    // which has been uploaded.
    for (var slug in uploads) {
      let upload = uploads[slug]
      totalLoaded += upload.loaded
      totalSize += upload.size
    }

    let percent = (totalLoaded / totalSize) * 100

    // If more than one: "Uploading files (5)..."
    // If only one: "Uploading myfile.txt..."
    let text = 'Uploading ' + (numberUploading == 1 ? `'${uploads[Object.keys(uploads)[0]].name}'` : `files (${numberUploading})`) + '...'

    return (
      <div className="alert alert-info progress animated fadeInUp ">
        <button type="button" className="close" onClick={ this.showAbort.bind(this) }>
          <span>×</span>
        </button>
        <div className="text-center">
          <small>{ text }</small>
        </div>
        <ProgressBar now={ percent } />
        <div className="text-center">
          <small>{ humanize.filesize(totalLoaded) } ({ percent.toFixed(2) } %)</small>
        </div>
      </div>
    )
  }
}

export default connect(state => {
  return {
    uploads: state.uploads,
    showAbortModal: state.showAbortModal
  }
})(UploadModal)
