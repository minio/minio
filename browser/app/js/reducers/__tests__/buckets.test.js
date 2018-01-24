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

import reducer from "../buckets"
import * as actions from "../../actions/buckets"

describe("buckets reducer", () => {
  it("should return the initial state", () => {
    expect(reducer(undefined, {})).toEqual({
      list: [],
      filter: "",
      currentBucket: ""
    })
  })

  it("should handle SET_BUCKETS", () => {
    expect(
      reducer(undefined, { type: actions.SET_LIST, buckets: ["bk1", "bk2"] })
    ).toEqual({
      list: ["bk1", "bk2"],
      filter: "",
      currentBucket: ""
    })
  })

  it("should handle SET_BUCKETS_FILTER", () => {
    expect(
      reducer(undefined, {
        type: actions.SET_FILTER,
        filter: "test"
      })
    ).toEqual({
      list: [],
      filter: "test",
      currentBucket: ""
    })
  })

  it("should handle SELECT_BUCKET", () => {
    expect(
      reducer(undefined, {
        type: actions.SET_CURRENT_BUCKET,
        bucket: "test"
      })
    ).toEqual({
      list: [],
      filter: "",
      currentBucket: "test"
    })
  })
})
