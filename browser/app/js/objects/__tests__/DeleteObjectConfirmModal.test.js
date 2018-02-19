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
import { shallow } from "enzyme"
import { DeleteObjectConfirmModal } from "../DeleteObjectConfirmModal"

describe("DeleteObjectConfirmModal", () => {
  it("should render without crashing", () => {
    shallow(<DeleteObjectConfirmModal />)
  })

  it("should call deleteObject when Delete is clicked", () => {
    const deleteObject = jest.fn()
    const wrapper = shallow(
      <DeleteObjectConfirmModal deleteObject={deleteObject} />
    )
    wrapper.find("ConfirmModal").prop("okHandler")()
    expect(deleteObject).toHaveBeenCalled()
  })

  it("should call hideDeleteConfirmModal when Cancel is clicked", () => {
    const hideDeleteConfirmModal = jest.fn()
    const wrapper = shallow(
      <DeleteObjectConfirmModal
        hideDeleteConfirmModal={hideDeleteConfirmModal}
      />
    )
    wrapper.find("ConfirmModal").prop("cancelHandler")()
    expect(hideDeleteConfirmModal).toHaveBeenCalled()
  })
})
