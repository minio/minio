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
import { connect } from "react-redux"
import { Dropdown } from "react-bootstrap"
import DeleteObjectConfirmModal from "./DeleteObjectConfirmModal"
import * as actions from "./actions"

export class PrefixActions extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      showDeleteConfirmation: false,
    }
  }
  handleDownload(e) {
    e.preventDefault()
    const { object, downloadPrefix } = this.props
    downloadPrefix(object.name)
  }
  deleteObject() {
    const { object, deleteObject } = this.props
    deleteObject(object.name)
  }
  showDeleteConfirmModal(e) {
    e.preventDefault()
    this.setState({ showDeleteConfirmation: true })
  }
  hideDeleteConfirmModal() {
    this.setState({
      showDeleteConfirmation: false,
    })
  }
  render() {
    const { object, showShareObjectModal, shareObjectName } = this.props
    return (
      <Dropdown id={`obj-actions-${object.name}`}>
        <Dropdown.Toggle noCaret className="fia-toggle" />
        <Dropdown.Menu>
          <a
            href=""
            className="fiad-action"
            title="Download as zip"
            onClick={this.handleDownload.bind(this)}
          >
            <i className="fas fa-cloud-download-alt" />
          </a>
          <a
            href=""
            className="fiad-action"
            title="Delete"
            onClick={this.showDeleteConfirmModal.bind(this)}
          >
            <i className="fas fa-trash-alt" />
          </a>
        </Dropdown.Menu>
        {this.state.showDeleteConfirmation && (
          <DeleteObjectConfirmModal
            deleteObject={this.deleteObject.bind(this)}
            hideDeleteConfirmModal={this.hideDeleteConfirmModal.bind(this)}
          />
        )}
      </Dropdown>
    )
  }
}

const mapStateToProps = (state, ownProps) => {
  return {
    object: ownProps.object,
  }
}

const mapDispatchToProps = (dispatch) => {
  return {
    downloadPrefix: object => dispatch(actions.downloadPrefix(object)),
    deleteObject: (object) => dispatch(actions.deleteObject(object)),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(PrefixActions)
