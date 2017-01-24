/*
 * Minio Browser (C) 2016 Minio, Inc.
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

/*
import React from 'react'
import ReactTestUtils, {renderIntoDocument} from 'react-addons-test-utils'

import expect from 'expect'
import Login from '../Login'

describe('Login', () => {
  it('it should have empty credentials', () => {
    const alert = {
      show: false
    }
    const dispatch = () => {}
    let loginComponent = renderIntoDocument(<Login alert={alert} dispatch={dispatch} />)
    const accessKey = document.getElementById('accessKey')
    const secretKey = document.getElementById('secretKey')
    // Validate default value.
    expect(accessKey.value).toEqual('')
    expect(secretKey.value).toEqual('')
  })
  it('it should set accessKey and secretKey', () => {
    const alert = {
      show: false
    }
    const dispatch = () => {}
    let loginComponent = renderIntoDocument(<Login alert={alert} dispatch={dispatch} />)
    let accessKey = loginComponent.refs.accessKey
    let secretKey = loginComponent.refs.secretKey
    accessKey.value = 'demo-username'
    secretKey.value = 'demo-password'
    ReactTestUtils.Simulate.change(accessKey)
    ReactTestUtils.Simulate.change(secretKey)
    // Validate if the change has occurred.
    expect(loginComponent.refs.accessKey.value).toEqual('demo-username')
    expect(loginComponent.refs.secretKey.value).toEqual('demo-password')
  })
});
*/
