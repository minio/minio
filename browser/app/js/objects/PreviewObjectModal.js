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
              <object data={this.state.url} style={{ display: "block", width: "100%" }}>
                <h3 style={{ textAlign: "center", display: "block", width: "100%" }}>
                  Do not have read permissions to preview "{this.props.object.name}"
                </h3>
              </object>
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
