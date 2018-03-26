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
