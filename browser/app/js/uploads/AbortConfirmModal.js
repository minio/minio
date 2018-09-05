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
import * as uploadsActions from "./actions"

import iconDanger from "../../img/icons/danger.svg"

export class AbortConfirmModal extends React.Component {
  abortUploads() {
    const { abort, uploads } = this.props
    for (var slug in uploads) {
      abort(slug)
    }
  }
  render() {
    const { hideAbort } = this.props
    return (
      <div className="modal modal--dialog">
        <div className="modal__content">
          <div className="modal__icon">
            <img src={iconDanger} alt="" />
          </div>
          <div className="modal__title">Abort uploads in progress?</div>
          <div className="modal__sub-title">This cannot be undone!</div>

          <div className="modal__actions">
            <button className="button button--light" onClick={hideAbort}>
              Upload
            </button>
            <button
              className="button button--danger"
              onClick={this.abortUploads.bind(this)}
            >
              Abort
            </button>
          </div>
        </div>
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    uploads: state.uploads.files
  }
}

const mapDispatchToProps = dispatch => {
  return {
    abort: slug => dispatch(uploadsActions.abortUpload(slug)),
    hideAbort: () => dispatch(uploadsActions.hideAbortModal())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(AbortConfirmModal)
