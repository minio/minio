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
