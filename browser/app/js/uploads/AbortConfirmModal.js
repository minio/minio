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
import classNames from "classnames"
import { connect } from "react-redux"
import ConfirmModal from "../browser/ConfirmModal"
import * as uploadsActions from "./actions"

export class AbortConfirmModal extends React.Component {
  abortUploads() {
    const { abort, uploads } = this.props
    for (var slug in uploads) {
      abort(slug)
    }
  }
  render() {
    const { hideAbort } = this.props
    let baseClass = classNames({
      "abort-upload": true
    })
    let okIcon = classNames({
      fas: true,
      "fa-times": true
    })
    let cancelIcon = classNames({
      fas: true,
      "fa-cloud-upload-alt": true
    })

    return (
      <ConfirmModal
        show={true}
        baseClass={baseClass}
        text="Abort uploads in progress?"
        icon="fas fa-info-circle mci-amber"
        sub="This cannot be undone!"
        okText="Abort"
        okIcon={okIcon}
        cancelText="Upload"
        cancelIcon={cancelIcon}
        okHandler={this.abortUploads.bind(this)}
        cancelHandler={hideAbort}
      />
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
