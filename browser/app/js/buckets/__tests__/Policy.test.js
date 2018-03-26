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
import { Policy } from "../Policy"
import { READ_ONLY, WRITE_ONLY, READ_WRITE } from "../../constants"
import web from "../../web"

jest.mock("../../web", () => ({
  SetBucketPolicy: jest.fn(() => {
    return Promise.resolve()
  })
}))

describe("Policy", () => {
  it("should render without crashing", () => {
    shallow(<Policy currentBucket={"bucket"} prefix={"foo"} policy={READ_ONLY} />)
  })

  it("should call web.setBucketPolicy and fetchPolicies on submit", () => {
    const fetchPolicies = jest.fn()
    const wrapper = shallow(
      <Policy 
        currentBucket={"bucket"}
        prefix={"foo"}
        policy={READ_ONLY}
        fetchPolicies={fetchPolicies}
      />
    )
    wrapper.find("button").simulate("click", { preventDefault: jest.fn() })

    expect(web.SetBucketPolicy).toHaveBeenCalledWith({
      bucketName: "bucket",
      prefix: "foo",
      policy: "none"
    })
    
    setImmediate(() => {
      expect(fetchPolicies).toHaveBeenCalledWith("bucket")
    })
  })

  it("should change the empty string to '*' while displaying prefixes", () => {
    const wrapper = shallow(
      <Policy currentBucket={"bucket"} prefix={""} policy={READ_ONLY} />
    )
    expect(wrapper.find(".pmbl-item").at(0).text()).toEqual("*")
  })
})
