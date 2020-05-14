/*
 * MinIO Cloud Storage (C) 2020 MinIO, Inc.
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
import { Modal, ModalHeader, ModalBody } from "react-bootstrap"

class PreviewObjectModal extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      url: "",
    }
  }

  componentDidMount() {
    this.props.getObjectURL(this.props.object.name, (url) => {
      this.setState({
        url: url,
      })
    })
  }

  render() {
    const { hidePreviewModal } = this.props
    return (
      <Modal
        show={true}
        animation={false}
        onHide={hidePreviewModal}
        bsSize="large"
      >
        <ModalHeader>Preview</ModalHeader>
        <ModalBody>
          <div className="input-group">
            {this.state.url && (
              <img
                alt="Image broken"
                src={this.state.url}
                style={{ display: "block", width: "100%" }}
              />
            )}
          </div>
        </ModalBody>
        <div className="modal-footer">
          {
            <button className="btn btn-link" onClick={hidePreviewModal}>
              Cancel
            </button>
          }
        </div>
      </Modal>
    )
  }
}
export default PreviewObjectModal
