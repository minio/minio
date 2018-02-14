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
import ConfirmModal from "../browser/ConfirmModal"

export const DeleteObjectConfirmModal = ({
  deleteObject,
  hideDeleteConfirmModal
}) => (
  <ConfirmModal
    show={true}
    icon="fa fa-exclamation-triangle mci-red"
    text="Are you sure you want to delete?"
    sub="This cannot be undone!"
    okText="Delete"
    cancelText="Cancel"
    okHandler={deleteObject}
    cancelHandler={hideDeleteConfirmModal}
  />
)

export default DeleteObjectConfirmModal
