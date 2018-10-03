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
import ShareObjectModal from "./ShareObjectModal"
import web from "../web"
import * as objectsActions from "./actions"
import { CSSTransition } from "react-transition-group"

import {
  SHARE_OBJECT_EXPIRY_DAYS,
  SHARE_OBJECT_EXPIRY_HOURS,
  SHARE_OBJECT_EXPIRY_MINUTES
} from "../constants"

export class ObjectsBulkActions extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      showDeleteConfirmation: false
    }
    this.clearSelected = this.clearSelected.bind(this)
  }
  handleDownload() {
    const { checkedObjects, downloadChecked, downloadObject } = this.props
    if (checkedObjects.length === 1 && !checkedObjects[0].endsWith("/")) {
      downloadObject(checkedObjects[0])
    } else {
      downloadChecked()
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

  clearSelected(e) {
    const { clearChecked, showShareObjectModal } = this.props

    if (!this.state.showDeleteConfirmation && !showShareObjectModal) {
      if (e.keyCode === 27) {
        clearChecked()
      }
    }
  }
  onActionsEnter() {
    document.addEventListener("keydown", this.clearSelected, false)
  }
  onActionsExit() {
    document.removeEventListener("keydown", this.clearSelected, false)
  }

  render() {
    const {
      checkedObjects,
      checkedObjectsCount,
      object,
      showShareObjectModal,
      clearChecked
    } = this.props
    const loggedIn = web.LoggedIn()

    const icons = {
      share: (
        <svg width="13" height="14">
          <path d="M10.338 9.31c-.507 0-.96.196-1.307.503l-4.753-2.71c.033-.15.06-.3.06-.457s-.027-.307-.06-.457l4.7-2.682c.36.326.833.53 1.36.53 1.107 0 2-.874 2-1.958s-.893-1.958-2-1.958-2 .874-2 1.958a2.11 2.11 0 0 0 .06.457l-4.7 2.682c-.36-.326-.833-.53-1.36-.53-1.107 0-2 .874-2 1.958s.893 1.958 2 1.958a2.01 2.01 0 0 0 1.36-.529L8.445 10.8c-.033.137-.053.28-.053.424-.001 1.05.873 1.906 1.946 1.906s1.947-.855 1.947-1.906-.874-1.905-1.947-1.905z" />
        </svg>
      ),
      download: (
        <svg width="12" height="14">
          <path d="M11.338 5.088H8.195V.5H3.48v4.588H.338l5.5 5.353 5.5-5.353zm-11 6.883v1.53h11v-1.53h-11z" />
        </svg>
      ),
      trash: (
        <svg width="11" height="14">
          <path d="M1.214 12.022c0 .813.643 1.478 1.43 1.478h5.714c.786 0 1.43-.665 1.43-1.478v-8.87H1.214v8.87zM10.5 1.163H8L7.286.5H3.714L3 1.163H.5V2.5h10V1.163z" />
        </svg>
      ),
      close: (
        <svg width="12" height="12">
          <path d="M11.316 1.948L10.318.95l-3.96 3.96L2.398.95l-.998.998 3.96 3.96-3.96 3.96 1 1 3.96-3.96 3.96 3.96 1-1-3.96-3.96z" />
        </svg>
      )
    }

    let downloadLabel
    if (checkedObjects.length === 1 && checkedObjects[0].endsWith("/")) {
      downloadLabel = "Download as zip"
    } else if (checkedObjectsCount > 1) {
      downloadLabel = "Download all as zip"
    } else {
      downloadLabel = "Download object"
    }

    const actionsProps = {
      mountOnEnter: true,
      unmountOnExit: true,
      timeout: {
        enter: 10,
        exit: 300
      },
      classNames: {
        enter: "object-actions--enter",
        enterDone: "object-actions--enter-done",
        exit: "object-actions--exit",
        exitDone: "object-actions--exit-done"
      },
      onEntered: this.onActionsEnter.bind(this),
      onExit: this.onActionsExit.bind(this)
    }

    return (
      <React.Fragment>
        <CSSTransition in={checkedObjectsCount > 0} {...actionsProps}>
          <div className="object-actions">
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
              {icons.trash}
              <span>Delete selected</span>
            </button>
            <button
              id="download-checked"
              className="object-actions__item"
              onClick={this.handleDownload.bind(this)}
              disabled={!checkedObjectsCount}
            >
              {icons.download}
              <span>{downloadLabel}</span>
            </button>
            {loggedIn &&
              checkedObjectsCount == 1 &&
              !checkedObjects[0].endsWith("/") && (
                <button
                  id="share-checked"
                  className="object-actions__item"
                  onClick={this.shareObject.bind(this)}
                  disabled={checkedObjectsCount != 1}
                >
                  {icons.share}
                  <span>Share</span>
                </button>
              )}
            <button
              id="close-bulk-actions"
              className="object-actions__item object-actions__close"
              onClick={clearChecked}
            >
              {icons.close}
            </button>
          </div>
        </CSSTransition>
        <ShareObjectModal
          showShareObject={showShareObjectModal}
          object={object}
        />

        <DeleteObjectConfirmModal
          deleteObject={this.deleteChecked.bind(this)}
          showDeleteConfirmModal={this.state.showDeleteConfirmation}
          hideDeleteConfirmModal={this.hideDeleteConfirmModal.bind(this)}
        />
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
    downloadObject: object => dispatch(actions.downloadObject(object)),
    downloadChecked: () => dispatch(actions.downloadCheckedObjects()),
    clearChecked: () => dispatch(actions.resetCheckedList()),
    deleteChecked: () => dispatch(actions.deleteCheckedObjects()),
    shareObject: (object, days, hours, minutes) =>
      dispatch(objectsActions.shareObject(object, days, hours, minutes))
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ObjectsBulkActions)
