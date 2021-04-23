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
import { shallow, mount } from "enzyme"
import Alert from "../Alert"

describe("Alert", () => {
  it("should render without crashing", () => {
    shallow(<Alert />)
  })

  it("should call onDismiss when close button is clicked", () => {
    const onDismiss = jest.fn()
    const wrapper = mount(
      <Alert show={true} type="danger" message="test" onDismiss={onDismiss} />
    )
    wrapper.find("button").simulate("click", { preventDefault: jest.fn() })
    expect(onDismiss).toHaveBeenCalled()
  })
})
