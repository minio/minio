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

import JSONrpc from "../jsonrpc"

describe("jsonrpc", () => {
  it("should fail with invalid endpoint", done => {
    try {
      let jsonRPC = new JSONrpc({
        endpoint: "htt://localhost:9000",
        namespace: "Test"
      })
    } catch (e) {
      done()
    }
  })
  it("should succeed with valid endpoint", () => {
    let jsonRPC = new JSONrpc({
      endpoint: "http://localhost:9000/webrpc",
      namespace: "Test"
    })
    expect(jsonRPC.version).toEqual("2.0")
    expect(jsonRPC.host).toEqual("localhost")
    expect(jsonRPC.port).toEqual("9000")
    expect(jsonRPC.path).toEqual("/webrpc")
    expect(jsonRPC.scheme).toEqual("http")
  })
})
