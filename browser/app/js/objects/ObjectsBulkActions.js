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
import classNames from "classnames"
import * as actions from "./actions"
import { getCheckedList } from "./selectors"
import DeleteObjectConfirmModal from "./DeleteObjectConfirmModal"

export class ObjectsBulkActions extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      showDeleteConfirmation: false
    }
  }
  deleteChecked() {
    const { deleteChecked } = this.props
    deleteChecked()
    this.hideDeleteConfirmModal()
  }
  hideDeleteConfirmModal() {
    this.setState({
      showDeleteConfirmation: false
    })
  }
  render() {
    const { checkedObjectsCount, downloadChecked, clearChecked } = this.props
    return (
      <div
        className={
          "list-actions" +
          classNames({
            " list-actions-toggled": checkedObjectsCount > 0
          })
        }
      >
        <span className="la-label">
          <i className="fa fa-check-circle" /> {checkedObjectsCount} Objects
          selected
        </span>
        <span className="la-actions pull-right">
          <button id="download-checked" onClick={downloadChecked}>
            {" "}
            Download all as zip{" "}
          </button>
        </span>
        <span className="la-actions pull-right">
          <button
            id="delete-checked"
            onClick={() => this.setState({ showDeleteConfirmation: true })}
          >
            {" "}
            Delete selected{" "}
          </button>
        </span>
        <i
          className="la-close fa fa-times"
          id="close-bulk-actions"
          onClick={clearChecked}
        />
        {this.state.showDeleteConfirmation && (
          <DeleteObjectConfirmModal
            deleteObject={this.deleteChecked.bind(this)}
            hideDeleteConfirmModal={this.hideDeleteConfirmModal.bind(this)}
          />
        )}
      </div>
    )
  }
}

const mapStateToProps = state => {
  return {
    checkedObjectsCount: getCheckedList(state).length
  }
}

const mapDispatchToProps = dispatch => {
  return {
    downloadChecked: () => dispatch(actions.downloadCheckedObjects()),
    clearChecked: () => dispatch(actions.resetCheckedList()),
    deleteChecked: () => dispatch(actions.deleteCheckedObjects())
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ObjectsBulkActions)
