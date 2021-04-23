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
import { connect } from "react-redux"
import { Modal, ModalBody } from "react-bootstrap"
import * as actionsBuckets from "./actions"

export class MakeBucketModal extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      bucketName: ""
    }
  }
  onSubmit(e) {
    e.preventDefault()
    const { makeBucket } = this.props
    const bucket = this.state.bucketName
    if (bucket) {
      makeBucket(bucket)
      this.hideModal()
    }
  }
  hideModal() {
    this.setState({
      bucketName: ""
    })
    this.props.hideMakeBucketModal()
  }
  render() {
    const { showMakeBucketModal } = this.props
    return (
      <Modal
        className="modal-create-bucket"
        bsSize="small"
        animation={false}
        show={showMakeBucketModal}
        onHide={this.hideModal.bind(this)}
      >
        <button className="close close-alt" onClick={this.hideModal.bind(this)}>
          <span>×</span>
        </button>
        <ModalBody>
          <form onSubmit={this.onSubmit.bind(this)}>
            <div className="input-group">
              <input
                className="ig-text"
                type="text"
                placeholder="Bucket Name"
                value={this.state.bucketName}
                onChange={e => this.setState({ bucketName: e.target.value })}
                autoFocus
              />
              <i className="ig-helpers" />
            </div>
          </form>
        </ModalBody>
      </Modal>
    )
  }
}

const mapStateToProps = state => {
  return {
    showMakeBucketModal: state.buckets.showMakeBucketModal
  }
}

const mapDispatchToProps = dispatch => {
  return {
    makeBucket: bucket => dispatch(actionsBuckets.makeBucket(bucket)),
    hideMakeBucketModal: () => dispatch(actionsBuckets.hideMakeBucketModal())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(MakeBucketModal)
