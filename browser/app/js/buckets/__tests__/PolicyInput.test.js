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
import { PolicyInput } from "../PolicyInput"
import { READ_ONLY, WRITE_ONLY, READ_WRITE } from "../../constants"
import web from "../../web"

jest.mock("../../web", () => ({
  SetBucketPolicy: jest.fn(() => {
    return Promise.resolve()
  })
}))

describe("PolicyInput", () => {
  it("should render without crashing", () => {
    const fetchPolicies = jest.fn()
    shallow(<PolicyInput currentBucket={"bucket"} fetchPolicies={fetchPolicies}/>)
  })

  it("should call fetchPolicies after the component has mounted", () => {
    const fetchPolicies = jest.fn()
    const wrapper = shallow(
      <PolicyInput currentBucket={"bucket"} fetchPolicies={fetchPolicies} />
    )
    setImmediate(() => {
      expect(fetchPolicies).toHaveBeenCalled()
    })
  })

  it("should call web.setBucketPolicy and fetchPolicies on submit", () => {
    const fetchPolicies = jest.fn()
    const wrapper = shallow(
      <PolicyInput currentBucket={"bucket"} policies={[]} fetchPolicies={fetchPolicies}/>
    )
    wrapper.instance().prefix = { value: "baz" }
    wrapper.instance().policy = { value: READ_ONLY }
    wrapper.find("button").simulate("click", { preventDefault: jest.fn() })

    expect(web.SetBucketPolicy).toHaveBeenCalledWith({
      bucketName: "bucket",
      prefix: "baz",
      policy: READ_ONLY
    })

    setImmediate(() => {
      expect(fetchPolicies).toHaveBeenCalledWith("bucket")
    })
  })

  it("should change the prefix '*' to an empty string", () => {
    const fetchPolicies = jest.fn()
    const wrapper = shallow(
      <PolicyInput currentBucket={"bucket"} policies={[]} fetchPolicies={fetchPolicies}/>
    )
    wrapper.instance().prefix = { value: "*" }
    wrapper.instance().policy = { value: READ_ONLY }

    wrapper.find("button").simulate("click", { preventDefault: jest.fn() })

    expect(wrapper.instance().prefix).toEqual({ value: "" })
  })
})
