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
