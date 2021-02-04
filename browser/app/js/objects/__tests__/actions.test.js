/*
 * MinIO Cloud Storage (C) 2018 MinIO, Inc.
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
import * as actionsObjects from "../actions"
import * as alertActions from "../../alert/actions"
import {
  minioBrowserPrefix,
  SORT_BY_NAME,
  SORT_ORDER_ASC,
  SORT_BY_LAST_MODIFIED,
  SORT_ORDER_DESC
} from "../../constants"
import history from "../../history"

jest.mock("../../web", () => ({
  LoggedIn: jest
    .fn(() => true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(false)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(true)
    .mockReturnValueOnce(false),
  ListObjects: jest.fn(({ bucketName }) => {
    if (bucketName === "test-deny") {
      return Promise.reject({
        message: "listobjects is denied"
      })
    } else {
      return Promise.resolve({
        objects: [{ name: "test1" }, { name: "test2" }],
        writable: false
      })
    }
  }),
  RemoveObject: jest.fn(({ bucketName, objects }) => {
    if (!bucketName) {
      return Promise.reject({ message: "Invalid bucket" })
    }
    return Promise.resolve({})
  }),
  PresignedGet: jest.fn(({ bucket, object }) => {
    if (!bucket) {
      return Promise.reject({ message: "Invalid bucket" })
    }
    return Promise.resolve({ url: "https://test.com/bk1/pre1/b.txt" })
  }),
  CreateURLToken: jest
    .fn()
    .mockImplementationOnce(() => {
      return Promise.resolve({ token: "test" })
    })
    .mockImplementationOnce(() => {
      return Promise.reject({ message: "Error in creating token" })
    })
    .mockImplementationOnce(() => {
      return Promise.resolve({ token: "test" })
    })
    .mockImplementationOnce(() => {
      return Promise.resolve({ token: "test" })
    }),
  GetBucketPolicy: jest.fn(({ bucketName, prefix }) => {
    if (!bucketName) {
      return Promise.reject({ message: "Invalid bucket" })
    }
    if (bucketName === 'test-public') return Promise.resolve({ policy: 'readonly' })
    return Promise.resolve({})
  })
}))

const middlewares = [thunk]
const mockStore = configureStore(middlewares)

describe("Objects actions", () => {
  it("creates objects/SET_LIST action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/SET_LIST",
        objects: [{ name: "test1" }, { name: "test2" }]
      }
    ]
    store.dispatch(
      actionsObjects.setList([{ name: "test1" }, { name: "test2" }])
    )
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/SET_SORT_BY action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/SET_SORT_BY",
        sortBy: SORT_BY_NAME
      }
    ]
    store.dispatch(actionsObjects.setSortBy(SORT_BY_NAME))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/SET_SORT_ORDER action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/SET_SORT_ORDER",
        sortOrder: SORT_ORDER_ASC
      }
    ]
    store.dispatch(actionsObjects.setSortOrder(SORT_ORDER_ASC))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/SET_LIST after fetching the objects", () => {
    const store = mockStore({
      buckets: { currentBucket: "bk1" },
      objects: { currentPrefix: "" }
    })
    const expectedActions = [
      {
        type: "objects/RESET_LIST"
      },
      { listLoading: true, type: "objects/SET_LIST_LOADING" },
      {
        type: "objects/SET_SORT_BY",
        sortBy: SORT_BY_LAST_MODIFIED
      },
      {
        type: "objects/SET_SORT_ORDER",
        sortOrder: SORT_ORDER_DESC
      },
      {
        type: "objects/SET_LIST",
        objects: [{ name: "test2" }, { name: "test1" }]
      },
      {
        type: "objects/SET_PREFIX_WRITABLE",
        prefixWritable: false
      },
      { listLoading: false, type: "objects/SET_LIST_LOADING" }
    ]
    return store.dispatch(actionsObjects.fetchObjects()).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })

  it("creates objects/RESET_LIST after failing to fetch the objects from bucket with ListObjects denied for LoggedIn users", () => {
    const store = mockStore({
      buckets: { currentBucket: "test-deny" },
      objects: { currentPrefix: "" }
    })
    const expectedActions = [
      {
        type: "objects/RESET_LIST"
      },
      { listLoading: true, type: "objects/SET_LIST_LOADING" },
      {
        type: "alert/SET",
        alert: {
          type: "danger",
          message: "listobjects is denied",
          id: alertActions.alertId,
          autoClear: true
        }
      },
      {
        type: "objects/RESET_LIST"
      },
      { listLoading: false, type: "objects/SET_LIST_LOADING" }
    ]
    return store.dispatch(actionsObjects.fetchObjects()).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })

  it("redirect to login after failing to fetch the objects from bucket for non-LoggedIn users", () => {
    const store = mockStore({
      buckets: { currentBucket: "test-deny" },
      objects: { currentPrefix: "" }
    })
    return store.dispatch(actionsObjects.fetchObjects()).then(() => {
      expect(history.location.pathname.endsWith("/login")).toBeTruthy()
    })
  })

  it("creates objects/SET_SORT_BY and objects/SET_SORT_ORDER when sortObjects is called", () => {
    const store = mockStore({
      objects: {
        list: [],
        sortBy: "",
        sortOrder: SORT_ORDER_ASC
      }
    })
    const expectedActions = [
      {
        type: "objects/SET_SORT_BY",
        sortBy: SORT_BY_NAME
      },
      {
        type: "objects/SET_SORT_ORDER",
        sortOrder: SORT_ORDER_ASC
      },
      {
        type: "objects/SET_LIST",
        objects: []
      }
    ]
    store.dispatch(actionsObjects.sortObjects(SORT_BY_NAME))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("should update browser url and creates objects/SET_CURRENT_PREFIX and objects/CHECKED_LIST_RESET actions when selectPrefix is called", () => {
    const store = mockStore({
      buckets: { currentBucket: "test" },
      objects: { currentPrefix: "" }
    })
    const expectedActions = [
      { type: "objects/SET_CURRENT_PREFIX", prefix: "abc/" },
      {
        type: "objects/RESET_LIST"
      },
      { listLoading: true, type: "objects/SET_LIST_LOADING" },
      { type: "objects/CHECKED_LIST_RESET" }
    ]
    store.dispatch(actionsObjects.selectPrefix("abc/"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
    expect(window.location.pathname.endsWith("/test/abc/")).toBeTruthy()
  })

  it("create objects/SET_PREFIX_WRITABLE action", () => {
    const store = mockStore()
    const expectedActions = [
      { type: "objects/SET_PREFIX_WRITABLE", prefixWritable: true }
    ]
    store.dispatch(actionsObjects.setPrefixWritable(true))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/REMOVE action", () => {
    const store = mockStore()
    const expectedActions = [{ type: "objects/REMOVE", object: "obj1" }]
    store.dispatch(actionsObjects.removeObject("obj1"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/REMOVE action when object is deleted", () => {
    const store = mockStore({
      buckets: { currentBucket: "test" },
      objects: { currentPrefix: "pre1/" }
    })
    const expectedActions = [{ type: "objects/REMOVE", object: "obj1" }]
    store.dispatch(actionsObjects.deleteObject("obj1")).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })

  it("creates alert/SET action when invalid bucket is provided", () => {
    const store = mockStore({
      buckets: { currentBucket: "" },
      objects: { currentPrefix: "pre1/" }
    })
    const expectedActions = [
      {
        type: "alert/SET",
        alert: {
          type: "danger",
          message: "Invalid bucket",
          id: alertActions.alertId
        }
      }
    ]
    return store.dispatch(actionsObjects.deleteObject("obj1")).then(() => {
      const actions = store.getActions()
      expect(actions).toEqual(expectedActions)
    })
  })

  it("creates objects/SET_SHARE_OBJECT action for showShareObject", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/SET_SHARE_OBJECT",
        show: true,
        object: "b.txt",
        url: "test",
        showExpiryDate: true
      }
    ]
    store.dispatch(actionsObjects.showShareObject("b.txt", "test"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/SET_SHARE_OBJECT action for hideShareObject", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/SET_SHARE_OBJECT",
        show: false,
        object: "",
        url: ""
      }
    ]
    store.dispatch(actionsObjects.hideShareObject())
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/SET_SHARE_OBJECT when object is shared", () => {
    const store = mockStore({
      buckets: { currentBucket: "bk1" },
      objects: { currentPrefix: "pre1/" },
      browser: { serverInfo: {} },
    })
    const expectedActions = [
      {
        type: "objects/SET_SHARE_OBJECT",
        show: true,
        object: "a.txt",
        url: "https://test.com/bk1/pre1/b.txt",
        showExpiryDate: true
      },
      {
        type: "alert/SET",
        alert: {
          type: "success",
          message: "Object shared. Expires in 1 days 0 hours 0 minutes",
          id: alertActions.alertId
        }
      }
    ]
    return store
      .dispatch(actionsObjects.shareObject("a.txt", 1, 0, 0))
      .then(() => {
        const actions = store.getActions()
        expect(actions).toEqual(expectedActions)
      })
  })

  it("creates objects/SET_SHARE_OBJECT when object is shared with public link", () => {
    const store = mockStore({
      buckets: { currentBucket: "test-public" },
      objects: { currentPrefix: "pre1/" },
      browser: { serverInfo: { info: { domains: ['public.com'] }} },
    })
    const expectedActions = [
      {
        type: "objects/SET_SHARE_OBJECT",
        show: true,
        object: "a.txt",
        url: "public.com/test-public/pre1/a.txt",
        showExpiryDate: false
      },
      {
        type: "alert/SET",
        alert: {
          type: "success",
          message: "Object shared.",
          id: alertActions.alertId
        }
      }
    ]
    return store
      .dispatch(actionsObjects.shareObject("a.txt", 1, 0, 0))
      .then(() => {
        const actions = store.getActions()
        expect(actions).toEqual(expectedActions)
      })
  })

  it("creates alert/SET when shareObject is failed", () => {
    const store = mockStore({
      buckets: { currentBucket: "" },
      objects: { currentPrefix: "pre1/" },
      browser: { serverInfo: {} },
    })
    const expectedActions = [
      {
        type: "alert/SET",
        alert: {
          type: "danger",
          message: "Invalid bucket",
          id: alertActions.alertId
        }
      }
    ]
    return store
      .dispatch(actionsObjects.shareObject("a.txt", 1, 0, 0))
      .then(() => {
        const actions = store.getActions()
        expect(actions).toEqual(expectedActions)
      })
  })

  describe("Download object", () => {
    it("should download the object non-LoggedIn users", () => {
      const setLocation = jest.fn()
      Object.defineProperty(window, "location", {
        set(url) {
          setLocation(url)
        },
        get() {
          return {
            origin: "http://localhost:8080"
          }
        }
      })
      const store = mockStore({
        buckets: { currentBucket: "bk1" },
        objects: { currentPrefix: "pre1/" }
      })
      store.dispatch(actionsObjects.downloadObject("obj1"))
      const url = `${
        window.location.origin
      }${minioBrowserPrefix}/download/bk1/${encodeURI("pre1/obj1")}?token=`
      expect(setLocation).toHaveBeenCalledWith(url)
    })

    it("should download the object for LoggedIn users", () => {
      const setLocation = jest.fn()
      Object.defineProperty(window, "location", {
        set(url) {
          setLocation(url)
        },
        get() {
          return {
            origin: "http://localhost:8080"
          }
        }
      })
      const store = mockStore({
        buckets: { currentBucket: "bk1" },
        objects: { currentPrefix: "pre1/" }
      })
      return store.dispatch(actionsObjects.downloadObject("obj1")).then(() => {
        const url = `${
          window.location.origin
        }${minioBrowserPrefix}/download/bk1/${encodeURI(
          "pre1/obj1"
        )}?token=test`
        expect(setLocation).toHaveBeenCalledWith(url)
      })
    })

    it("create alert/SET action when CreateUrlToken fails", () => {
      const store = mockStore({
        buckets: { currentBucket: "bk1" },
        objects: { currentPrefix: "pre1/" }
      })
      const expectedActions = [
        {
          type: "alert/SET",
          alert: {
            type: "danger",
            message: "Error in creating token",
            id: alertActions.alertId
          }
        }
      ]
      return store.dispatch(actionsObjects.downloadObject("obj1")).then(() => {
        const actions = store.getActions()
        expect(actions).toEqual(expectedActions)
      })
    })
  })

  it("should download prefix", () => {
    const open = jest.fn()
    const send = jest.fn()
    const xhrMockClass = () => ({
      open: open,
      send: send
    })
    window.XMLHttpRequest = jest.fn().mockImplementation(xhrMockClass)

    const store = mockStore({
      buckets: { currentBucket: "bk1" },
      objects: { currentPrefix: "pre1/" }
    })
    return store.dispatch(actionsObjects.downloadPrefix("pre2/")).then(() => {
      const requestUrl = `${
        location.origin
      }${minioBrowserPrefix}/zip?token=test`
      expect(open).toHaveBeenCalledWith("POST", requestUrl, true)
      expect(send).toHaveBeenCalledWith(
        JSON.stringify({
          bucketName: "bk1",
          prefix: "pre1/",
          objects: ["pre2/"]
        })
      )
    })
  })

  it("creates objects/CHECKED_LIST_ADD action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/CHECKED_LIST_ADD",
        object: "obj1"
      }
    ]
    store.dispatch(actionsObjects.checkObject("obj1"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/CHECKED_LIST_REMOVE action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/CHECKED_LIST_REMOVE",
        object: "obj1"
      }
    ]
    store.dispatch(actionsObjects.uncheckObject("obj1"))
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("creates objects/CHECKED_LIST_RESET action", () => {
    const store = mockStore()
    const expectedActions = [
      {
        type: "objects/CHECKED_LIST_RESET"
      }
    ]
    store.dispatch(actionsObjects.resetCheckedList())
    const actions = store.getActions()
    expect(actions).toEqual(expectedActions)
  })

  it("should download checked objects", () => {
    const open = jest.fn()
    const send = jest.fn()
    const xhrMockClass = () => ({
      open: open,
      send: send
    })
    window.XMLHttpRequest = jest.fn().mockImplementation(xhrMockClass)

    const store = mockStore({
      buckets: { currentBucket: "bk1" },
      objects: { currentPrefix: "pre1/", checkedList: ["obj1"] }
    })
    return store.dispatch(actionsObjects.downloadCheckedObjects()).then(() => {
      const requestUrl = `${
        location.origin
      }${minioBrowserPrefix}/zip?token=test`
      expect(open).toHaveBeenCalledWith("POST", requestUrl, true)
      expect(send).toHaveBeenCalledWith(
        JSON.stringify({
          bucketName: "bk1",
          prefix: "pre1/",
          objects: ["obj1"]
        })
      )
    })
  })
})
