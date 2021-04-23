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
import { BucketPolicyModal } from "../BucketPolicyModal"
import { READ_ONLY, WRITE_ONLY, READ_WRITE } from "../../constants"

describe("BucketPolicyModal", () => {
  it("should render without crashing", () => {
    shallow(<BucketPolicyModal policies={[]}/>)
  })

  it("should call hideBucketPolicy when close button is clicked", () => {
    const hideBucketPolicy = jest.fn()
    const wrapper = shallow(
      <BucketPolicyModal hideBucketPolicy={hideBucketPolicy} policies={[]} />
    )
    wrapper.find("button").simulate("click")
    expect(hideBucketPolicy).toHaveBeenCalled()
  })

  it("should include the PolicyInput and Policy components when there are any policies", () => {
    const wrapper = shallow(
      <BucketPolicyModal policies={ [{prefix: "test", policy: READ_ONLY}] } />
    )
    expect(wrapper.find("Connect(PolicyInput)").length).toBe(1)
    expect(wrapper.find("Connect(Policy)").length).toBe(1)
  })
})
