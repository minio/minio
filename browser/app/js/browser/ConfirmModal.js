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
