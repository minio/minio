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
