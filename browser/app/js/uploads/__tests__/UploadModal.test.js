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
import { UploadModal } from "../UploadModal"

describe("UploadModal", () => {
  it("should render without crashing", () => {
    shallow(<UploadModal uploads={{}} />)
  })

  it("should render AbortConfirmModal when showAbort is true", () => {
    const wrapper = shallow(<UploadModal uploads={{}} showAbort={true} />)
    expect(wrapper.find("Connect(AbortConfirmModal)").length).toBe(1)
  })

  it("should render nothing when there are no files being uploaded", () => {
    const wrapper = shallow(<UploadModal uploads={{}} />)
    expect(wrapper.find("noscript").length).toBe(1)
  })

  it("should show upload progress when one or more files are being uploaded", () => {
    const wrapper = shallow(
      <UploadModal
        uploads={{ "a-b/-test": { size: 100, loaded: 50, name: "test" } }}
      />
    )
    expect(wrapper.find("ProgressBar").length).toBe(1)
  })

  it("should call showAbortModal when close button is clicked", () => {
    const showAbortModal = jest.fn()
    const wrapper = shallow(
      <UploadModal
        uploads={{ "a-b/-test": { size: 100, loaded: 50, name: "test" } }}
        showAbortModal={showAbortModal}
      />
    )
    wrapper.find("button").simulate("click")
    expect(showAbortModal).toHaveBeenCalled()
  })
})
