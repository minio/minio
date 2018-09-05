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
import ShareObjectModal from "./ShareObjectModal"
import web from "../web"
import * as objectsActions from "./actions"
import {
  SHARE_OBJECT_EXPIRY_DAYS,
  SHARE_OBJECT_EXPIRY_HOURS,
  SHARE_OBJECT_EXPIRY_MINUTES
} from "../constants"

import iconDelete from "../../img/icons/trash.svg"
import iconShare from "../../img/icons/share.svg"
import iconDownload from "../../img/icons/download.svg"
import iconClose from "../../img/icons/close.svg"

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
  shareObject(e) {
    e.preventDefault()
    const { checkedObjects, shareObject } = this.props
    if (checkedObjects.length != 1) {
      return
    }
    const object = checkedObjects[0]
    shareObject(
      object,
      SHARE_OBJECT_EXPIRY_DAYS,
      SHARE_OBJECT_EXPIRY_HOURS,
      SHARE_OBJECT_EXPIRY_MINUTES
    )
  }

  render() {
    const {
      checkedObjects,
      checkedObjectsCount,
      downloadChecked,
      object,
      showShareObjectModal,
      clearChecked
    } = this.props
    const loggedIn = web.LoggedIn()

    return (
      <React.Fragment>
        <div
          className={classNames({
            "object-actions": true,
            "object-actions--active": checkedObjectsCount
          })}
        >
          <button
            id="delete-checked"
            className="object-actions__item"
            onClick={() =>
              this.setState({
                showDeleteConfirmation: true
              })
            }
            disabled={!checkedObjectsCount}
          >
            <img src={iconDelete} alt="" />
            Delete selected
          </button>

          {loggedIn && (
            <button
              id="share-checked"
              className="object-actions__item"
              onClick={this.shareObject.bind(this)}
              disabled={checkedObjectsCount != 1}
            >
              <img src={iconShare} alt="" />
              Share
            </button>
          )}
          <button
            id="download-checked"
            className="object-actions__item"
            onClick={downloadChecked}
            disabled={!checkedObjectsCount}
          >
            <img src={iconDownload} alt="" />
            Download
            {checkedObjects.length > 1 ? " all as zip" : " object"}
          </button>
          <span
            id="close-bulk-actions"
            className="object-actions__item object-actions__close"
            onClick={clearChecked}
          >
            <img src={iconClose} alt="" />
            Cancel
          </span>
        </div>

        {showShareObjectModal && <ShareObjectModal object={object} />}

        {this.state.showDeleteConfirmation && (
          <DeleteObjectConfirmModal
            deleteObject={this.deleteChecked.bind(this)}
            hideDeleteConfirmModal={this.hideDeleteConfirmModal.bind(this)}
          />
        )}
      </React.Fragment>
    )
  }
}

const mapStateToProps = state => {
  return {
    checkedObjects: getCheckedList(state),
    checkedObjectsCount: getCheckedList(state).length,
    showShareObjectModal: state.objects.shareObject.show
  }
}

const mapDispatchToProps = dispatch => {
  return {
    downloadChecked: () => dispatch(actions.downloadCheckedObjects()),
    clearChecked: () => dispatch(actions.resetCheckedList()),
    deleteChecked: () => dispatch(actions.deleteCheckedObjects()),
    shareObject: (object, days, hours, minutes) =>
      dispatch(objectsActions.shareObject(object, days, hours, minutes))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ObjectsBulkActions)
