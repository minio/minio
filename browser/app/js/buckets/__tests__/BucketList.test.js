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
import history from "../../history"
import { BucketList } from "../BucketList"

jest.mock("../../web", () => ({
  LoggedIn: jest
    .fn(() => false)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(true)
}))

describe("BucketList", () => {
  it("should render without crashing", () => {
    const fetchBuckets = jest.fn()
    shallow(<BucketList visibleBuckets={[]} fetchBuckets={fetchBuckets} />)
  })

  it("should call fetchBuckets before component is mounted", () => {
    const fetchBuckets = jest.fn()
    const wrapper = shallow(
      <BucketList visibleBuckets={[]} fetchBuckets={fetchBuckets} />
    )
    expect(fetchBuckets).toHaveBeenCalled()
  })

  it("should call setBucketList and selectBucket before component is mounted when the user has not loggedIn", () => {
    const setBucketList = jest.fn()
    const selectBucket = jest.fn()
    history.push("/bk1/pre1")
    const wrapper = shallow(
      <BucketList
        visibleBuckets={[]}
        setBucketList={setBucketList}
        selectBucket={selectBucket}
      />
    )
    expect(setBucketList).toHaveBeenCalledWith(["bk1"])
    expect(selectBucket).toHaveBeenCalledWith("bk1", "pre1")
  })
})
