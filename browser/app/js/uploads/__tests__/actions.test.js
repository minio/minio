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

import configureStore from "redux-mock-store"
import thunk from "redux-thunk"
import * as uploadsActions from "../actions"

const middlewares = [thunk]
const mockStore = configureStore(middlewares)

describe("Uploads actions", () => {
  it("creates uploads/ADD action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "uploads/ADD",
        slug: "a-b-c",
        size: 100,
        name: "test"
      }
    ]
    store.dispatch(uploadsActions.add("a-b-c", 100, "test"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates uploads/UPDATE_PROGRESS action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "uploads/UPDATE_PROGRESS",
        slug: "a-b-c",
        loaded: 50
      }
    ]
    store.dispatch(uploadsActions.updateProgress("a-b-c", 50))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates uploads/STOP action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "uploads/STOP",
        slug: "a-b-c"
      }
    ]
    store.dispatch(uploadsActions.stop("a-b-c"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates uploads/SHOW_ABORT_MODAL action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "uploads/SHOW_ABORT_MODAL",
        show: true
      }
    ]
    store.dispatch(uploadsActions.showAbortModal())
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  describe("uploadFile", () => {
    const file = new Blob(["file content"], {
      type: "text/plain"
    })
    file.name = "file1"

    it("creates alerts/SET action when currentBucket is not present", () => {
      const store = mockStore({
        buckets: { currentBucket: "" }
      })
      const expectedActions = [
        {
          type: "alert/SET",
          alert: {
            id: 0,
            type: "danger",
            message: "Please choose a bucket before trying to upload files."
          }
        }
      ]
      const file = new Blob(["file content"], { type: "text/plain" })
      store.dispatch(uploadsActions.uploadFile(file))
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })

    it("creates uploads/ADD action before uploading the file", () => {
      const store = mockStore({
        buckets: { currentBucket: "test1" },
        objects: { currentPrefix: "pre1/" }
      })
      const expectedActions = [
        {
          type: "uploads/ADD",
          slug: "test1-pre1/-file1",
          size: file.size,
          name: file.name
        }
      ]
      store.dispatch(uploadsActions.uploadFile(file))
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })

    it("should open and send XMLHttpRequest", () => {
      const open = jest.fn()
      const send = jest.fn()
      const xhrMockClass = () => ({
        open: open,
        send: send,
        setRequestHeader: jest.fn(),
        upload: {
          addEventListener: jest.fn()
        }
      })
      window.XMLHttpRequest = jest.fn().mockImplementation(xhrMockClass)
      const store = mockStore({
        buckets: { currentBucket: "test1" },
        objects: { currentPrefix: "pre1/" }
      })
      store.dispatch(uploadsActions.uploadFile(file))
      expect(open).toHaveBeenCalledWith(
        "PUT",
        "https://localhost:8080/upload/test1/pre1/file1",
        true
      )
      expect(send).toHaveBeenCalledWith(file)
    })
  })

  it("creates uploads/STOP and uploads/SHOW_ABORT_MODAL after abortUpload", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "uploads/STOP",
        slug: "a-b/-c"
      },
      {
        type: "uploads/SHOW_ABORT_MODAL",
        show: false
      }
    ]
    store.dispatch(uploadsActions.abortUpload("a-b/-c"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })
})
