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
import { Dropzone } from "../Dropzone"

describe("Dropzone", () => {
  it("should render without crashing", () => {
    shallow(<Dropzone />)
  })

  it("should call uploadFile with files", () => {
    const uploadFile = jest.fn()
    const wrapper = shallow(<Dropzone uploadFile={uploadFile} />)
    const file1 = new Blob(["file content1"], { type: "text/plain" })
    const file2 = new Blob(["file content2"], { type: "text/plain" })
    wrapper.first().prop("onDrop")([file1, file2])
    expect(uploadFile.mock.calls).toEqual([[file1], [file2]])
  })
})
