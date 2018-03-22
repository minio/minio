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
import * as actions from "./actions"
import { getCheckedList } from "./selectors"
import DeleteObjectConfirmModal from "./DeleteObjectConfirmModal"
import BrowserDropdown from "../browser/BrowserDropdown"
import SidebarToggle from "../browser/SidebarToggle"
import { minioBrowserPrefix } from "../constants"
import ShareObjectModal from "./ShareObjectModal"
import web from "../web"
import * as objectsActions from "./actions"
import {
  SHARE_OBJECT_EXPIRY_DAYS,
  SHARE_OBJECT_EXPIRY_HOURS,
  SHARE_OBJECT_EXPIRY_MINUTES
} from "../constants"

export class Toolbar extends React.Component {
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
      checkedObjectsCount,
      downloadChecked,
      object,
      showShareObjectModal
    } = this.props
    const loggedIn = web.LoggedIn()

    return (
      <div className="toolbar">
        <SidebarToggle />
        <button
          className="toolbar__item zmdi zmdi-delete"
          onClick={() =>
            this.setState({
              showDeleteConfirmation: true
            })
          }
          disabled={!checkedObjectsCount}
        />
        {loggedIn ? (
          <button
            className="toolbar__item zmdi zmdi-share"
            onClick={this.shareObject.bind(this)}
            disabled={checkedObjectsCount != 1}
          />
        ) : (
          ""
        )}
        <button
          className="toolbar__item zmdi zmdi-download"
          onClick={downloadChecked}
          disabled={!checkedObjectsCount}
        />
        {showShareObjectModal && <ShareObjectModal object={object} />}
        <div className="toolbar__end">
          {loggedIn ? (
            <BrowserDropdown />
          ) : (
            <a
              className="toolbar__item toolbar__item--alt btn btn--danger"
              href={minioBrowserPrefix + "/login"}
            >
              Login
            </a>
          )}
        </div>
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
    toggleSidebar: () => dispatch(actionsCommon.toggleSidebar()),
    shareObject: (object, days, hours, minutes) =>
      dispatch(objectsActions.shareObject(object, days, hours, minutes))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(Toolbar)
