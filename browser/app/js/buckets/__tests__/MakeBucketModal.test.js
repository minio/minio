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
import { MakeBucketModal } from "../MakeBucketModal"

describe("MakeBucketModal", () => {
  it("should render without crashing", () => {
    shallow(<MakeBucketModal />)
  })

  it("should call hideMakeBucketModal when close button is clicked", () => {
    const hideMakeBucketModal = jest.fn()
    const wrapper = shallow(
      <MakeBucketModal hideMakeBucketModal={hideMakeBucketModal} />
    )
    wrapper.find("button").simulate("click")
    expect(hideMakeBucketModal).toHaveBeenCalled()
  })

  it("bucketName should be cleared before hiding the modal", () => {
    const hideMakeBucketModal = jest.fn()
    const wrapper = shallow(
      <MakeBucketModal hideMakeBucketModal={hideMakeBucketModal} />
    )
    wrapper.find("input").simulate("change", {
      target: { value: "test" }
    })
    expect(wrapper.state("bucketName")).toBe("test")
    wrapper.find("button").simulate("click")
    expect(wrapper.state("bucketName")).toBe("")
  })

  it("should call makeBucket when the form is submitted", () => {
    const makeBucket = jest.fn()
    const hideMakeBucketModal = jest.fn()
    const wrapper = shallow(
      <MakeBucketModal
        makeBucket={makeBucket}
        hideMakeBucketModal={hideMakeBucketModal}
      />
    )
    wrapper.find("input").simulate("change", {
      target: { value: "test" }
    })
    wrapper.find("form").simulate("submit", { preventDefault: jest.fn() })
    expect(makeBucket).toHaveBeenCalledWith("test")
  })

  it("should call hideMakeBucketModal and clear bucketName after the form is submited", () => {
    const makeBucket = jest.fn()
    const hideMakeBucketModal = jest.fn()
    const wrapper = shallow(
      <MakeBucketModal
        makeBucket={makeBucket}
        hideMakeBucketModal={hideMakeBucketModal}
      />
    )
    wrapper.find("input").simulate("change", {
      target: { value: "test" }
    })
    wrapper.find("form").simulate("submit", { preventDefault: jest.fn() })
    expect(hideMakeBucketModal).toHaveBeenCalled()
    expect(wrapper.state("bucketName")).toBe("")
  })
})
