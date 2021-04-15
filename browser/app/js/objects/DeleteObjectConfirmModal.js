/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
import { withTranslation } from "react-i18next"

export const DeleteObjectConfirmModal = ({
  deleteObject,
  hideDeleteConfirmModal,
  t
}) => (
  <ConfirmModal
    show={true}
    icon="fas fa-exclamation-triangle mci-red"
    text={t('confirmationDelete')}
    sub={t('confirmationDeleteSub')}
    okText={t('common:delete')}
    cancelText={t('common:cancel')}
    okHandler={deleteObject}
    cancelHandler={hideDeleteConfirmModal}
  />
)

export default withTranslation('deleteObjectConfirmationModal')(DeleteObjectConfirmModal)
