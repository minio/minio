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
import { ObjectsList } from "../ObjectsList"

describe("ObjectsList", () => {
  it("should render without crashing", () => {
    shallow(<ObjectsList objects={[]} />)
  })

  it("should render ObjectContainer for every object", () => {
    const wrapper = shallow(
      <ObjectsList objects={[{ name: "test1.jpg" }, { name: "test2.jpg" }]} />
    )
    expect(wrapper.find("Connect(ObjectContainer)").length).toBe(2)
  })

  it("should render PrefixContainer for every prefix", () => {
    const wrapper = shallow(
      <ObjectsList objects={[{ name: "abc/" }, { name: "xyz/" }]} />
    )
    expect(wrapper.find("Connect(PrefixContainer)").length).toBe(2)
  })
})
