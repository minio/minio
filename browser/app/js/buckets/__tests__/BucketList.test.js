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
    shallow(<BucketList filteredBuckets={[]} fetchBuckets={fetchBuckets} />)
  })

  it("should call fetchBuckets before component is mounted", () => {
    const fetchBuckets = jest.fn()
    const wrapper = shallow(
      <BucketList filteredBuckets={[]} fetchBuckets={fetchBuckets} />
    )
    expect(fetchBuckets).toHaveBeenCalled()
  })

  it("should call setBucketList and selectBucket before component is mounted when the user has not loggedIn", () => {
    const setBucketList = jest.fn()
    const selectBucket = jest.fn()
    history.push("/bk1/pre1")
    const wrapper = shallow(
      <BucketList
        filteredBuckets={[]}
        setBucketList={setBucketList}
        selectBucket={selectBucket}
      />
    )
    expect(setBucketList).toHaveBeenCalledWith(["bk1"])
    expect(selectBucket).toHaveBeenCalledWith("bk1", "pre1")
  })
})
