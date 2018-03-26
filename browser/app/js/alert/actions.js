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

export const SET = "alert/SET"
export const CLEAR = "alert/CLEAR"

export let alertId = 0

export const set = alert => {
  const id = alertId++
  return (dispatch, getState) => {
    if (alert.type !== "danger") {
      setTimeout(() => {
        dispatch({
          type: CLEAR,
          alert: {
            id
          }
        })
      }, 5000)
    }
    dispatch({
      type: SET,
      alert: Object.assign({}, alert, {
        id
      })
    })
  }
}

export const clear = () => {
  return { type: CLEAR }
}
