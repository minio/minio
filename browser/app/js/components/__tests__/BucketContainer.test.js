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
import BucketContainer from "../BucketContainer"
import configureStore from "redux-mock-store"

const mockStore = configureStore()

describe("BucketContainer", () => {
  let store
  beforeEach(() => {
    store = mockStore({
      buckets: {
        currentBucket: "Test"
      }
    })
    store.dispatch = jest.fn()
  })
  
  it("should render without crashing", () => {
    shallow(<BucketContainer store={store}/>)
  })

  it('maps state and dispatch to props', () => {
    const wrapper = shallow(<BucketContainer store={store}/>)
    expect(wrapper.props()).toEqual(expect.objectContaining({
      isActive: expect.any(Boolean),
      selectBucket: expect.any(Function)
    }))
  })

  it('maps selectBucket to dispatch action', () => {
    const wrapper = shallow(<BucketContainer store={store}/>)
    wrapper.props().selectBucket()
    expect(store.dispatch).toHaveBeenCalled()
  })
})
