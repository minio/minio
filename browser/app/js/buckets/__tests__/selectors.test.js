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

import { getFilteredBuckets, getCurrentBucket } from "../selectors"

describe("getFilteredBuckets", () => {
  let state
  beforeEach(() => {
    state = {
      buckets: {
        list: ["test1", "test11", "test2"]
      }
    }
  })

  it("should return all buckets if no filter specified", () => {
    state.buckets.filter = ""
    expect(getFilteredBuckets(state)).toEqual(["test1", "test11", "test2"])
  })

  it("should return all matching buckets if filter is specified", () => {
    state.buckets.filter = "test1"
    expect(getFilteredBuckets(state)).toEqual(["test1", "test11"])
  })
})
