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
import { UploadModal } from "../UploadModal"

describe("UploadModal", () => {
  it("should render without crashing", () => {
    shallow(<UploadModal uploads={{}} />)
  })

  it("should render AbortConfirmModal when showAbort is true", () => {
    const wrapper = shallow(<UploadModal uploads={{}} showAbort={true} />)
    expect(wrapper.find("Connect(AbortConfirmModal)").length).toBe(1)
  })

  it("should render nothing when there are no files being uploaded", () => {
    const wrapper = shallow(<UploadModal uploads={{}} />)
    expect(wrapper.find("noscript").length).toBe(1)
  })

  it("should show upload progress when one or more files are being uploaded", () => {
    const wrapper = shallow(
      <UploadModal
        uploads={{ "a-b/-test": { size: 100, loaded: 50, name: "test" } }}
      />
    )
    expect(wrapper.find("ProgressBar").length).toBe(1)
  })

  it("should call showAbortModal when close button is clicked", () => {
    const showAbortModal = jest.fn()
    const wrapper = shallow(
      <UploadModal
        uploads={{ "a-b/-test": { size: 100, loaded: 50, name: "test" } }}
        showAbortModal={showAbortModal}
      />
    )
    wrapper.find("button").simulate("click")
    expect(showAbortModal).toHaveBeenCalled()
  })
})
