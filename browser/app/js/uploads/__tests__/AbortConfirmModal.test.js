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
import { AbortConfirmModal } from "../AbortConfirmModal"

describe("AbortConfirmModal", () => {
  it("should render without crashing", () => {
    shallow(<AbortConfirmModal />)
  })

  it("should call abort for every upload when Abort is clicked", () => {
    const abort = jest.fn()
    const wrapper = shallow(
      <AbortConfirmModal
        uploads={{
          "a-b/-test1": { size: 100, loaded: 50, name: "test1" },
          "a-b/-test2": { size: 100, loaded: 50, name: "test2" }
        }}
        abort={abort}
      />
    )
    wrapper.instance().abortUploads()
    expect(abort.mock.calls.length).toBe(2)
    expect(abort.mock.calls[0][0]).toBe("a-b/-test1")
    expect(abort.mock.calls[1][0]).toBe("a-b/-test2")
  })

  it("should call hideAbort when cancel is clicked", () => {
    const hideAbort = jest.fn()
    const wrapper = shallow(<AbortConfirmModal hideAbort={hideAbort} />)
    wrapper.find("ConfirmModal").prop("cancelHandler")()
    expect(hideAbort).toHaveBeenCalled()
  })
})
