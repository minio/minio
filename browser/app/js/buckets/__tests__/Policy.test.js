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
import { Policy } from "../Policy"
import { READ_ONLY, WRITE_ONLY, READ_WRITE, NONE } from "../../constants"
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

  it("should not render when policy is listed as 'none'", () => {
    const wrapper = shallow(<Policy currentBucket={"bucket"} prefix={"foo"} policy={NONE} />)
    expect(wrapper.find(".pmb-list").length).toBe(0)
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
