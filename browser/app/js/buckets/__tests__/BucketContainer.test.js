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
