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
import { shallow, mount } from "enzyme"
import { MainActions } from "../MainActions"

jest.mock("../../web", () => ({
  LoggedIn: jest
    .fn(() => true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(false)
    .mockReturnValueOnce(false)
}))

describe("MainActions", () => {
  it("should render without crashing", () => {
    shallow(<MainActions />)
  })

  it("should not show any actions when user has not LoggedIn and prefixWritable is false", () => {
    const wrapper = shallow(<MainActions />)
    expect(wrapper.find("#show-make-bucket").length).toBe(0)
    expect(wrapper.find("#file-input").length).toBe(0)
  })

  it("should show only file upload action when user has not LoggedIn and prefixWritable is true", () => {
    const wrapper = shallow(<MainActions prefixWritable={true} />)
    expect(wrapper.find("#show-make-bucket").length).toBe(0)
    expect(wrapper.find("#file-input").length).toBe(1)
  })

  it("should show make bucket upload file actions when user has LoggedIn", () => {
    const wrapper = shallow(<MainActions />)
    expect(wrapper.find("#show-make-bucket").length).toBe(1)
    expect(wrapper.find("#file-input").length).toBe(1)
  })

  it("should call showMakeBucketModal when create bucket icon is clicked", () => {
    const showMakeBucketModal = jest.fn()
    const wrapper = shallow(
      <MainActions showMakeBucketModal={showMakeBucketModal} />
    )
    wrapper
      .find("#show-make-bucket")
      .simulate("click", { preventDefault: jest.fn() })
    expect(showMakeBucketModal).toHaveBeenCalled()
  })

  it("should call uploadFile when a file is selected for upload", () => {
    const uploadFile = jest.fn()
    const wrapper = shallow(<MainActions uploadFile={uploadFile} />)
    const files = [new Blob(["file content"], { type: "text/plain" })]
    const input = wrapper.find("#file-input")
    const event = {
      preventDefault: jest.fn(),
      target: {
        files: {
          length: files.length,
          item: function(index) {
            return files[index]
          }
        }
      }
    }
    input.simulate("change", event)
    expect(uploadFile).toHaveBeenCalledWith(files[0])
  })
})
