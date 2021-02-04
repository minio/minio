/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
import { StorageInfo } from "../StorageInfo"

describe("StorageInfo", () => {
  it("should render without crashing", () => {
    shallow(
      <StorageInfo storageInfo={ {used: 60} } fetchStorageInfo={jest.fn()} />
    )
  })

  it("should fetchStorageInfo before component is mounted", () => {
    const fetchStorageInfo = jest.fn()
    shallow(
      <StorageInfo
        storageInfo={ {used: 60} }
        fetchStorageInfo={fetchStorageInfo}
      />
    )
    expect(fetchStorageInfo).toHaveBeenCalled()
  })

  it("should not render anything if used is null", () => {
    const fetchStorageInfo = jest.fn()
    const wrapper = shallow(
      <StorageInfo
      storageInfo={ {used: 0} }
        fetchStorageInfo={fetchStorageInfo}
      />
    )
    expect(wrapper.text()).toBe("")
  })
})
