/*
 * Minio Cloud Storage (C) 2016, 2018 Minio, Inc.
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
import { Modal, ModalBody } from "react-bootstrap"

let ConfirmModal = ({
  baseClass,
  icon,
  text,
  sub,
  okText,
  cancelText,
  okHandler,
  cancelHandler,
  show
}) => {
  return (
    <Modal
      bsSize="small"
      animation={false}
      show={show}
      className={"modal-confirm " + (baseClass || "")}
    >
      <ModalBody>
        <div className="mc-icon">
          <i className={icon} />
        </div>
        <div className="mc-text">{text}</div>
        <div className="mc-sub">{sub}</div>
      </ModalBody>
      <div className="modal-footer">
        <button className="btn btn-danger" onClick={okHandler}>
          {okText}
        </button>
        <button className="btn btn-link" onClick={cancelHandler}>
          {cancelText}
        </button>
      </div>
    </Modal>
  )
}

export default ConfirmModal
