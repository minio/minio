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
