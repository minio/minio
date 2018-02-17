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
import { ShareObjectModal } from "../ShareObjectModal"
import {
  SHARE_OBJECT_EXPIRY_DAYS,
  SHARE_OBJECT_EXPIRY_HOURS,
  SHARE_OBJECT_EXPIRY_MINUTES
} from "../../constants"

jest.mock("../../web", () => ({
  LoggedIn: jest.fn(() => {
    return true
  })
}))

describe("ShareObjectModal", () => {
  it("should render without crashing", () => {
    shallow(
      <ShareObjectModal
        object={{ name: "obj1" }}
        shareObjectDetails={{ show: true, object: "obj1", url: "test" }}
      />
    )
  })

  it("shoud call hideShareObject when Cancel is clicked", () => {
    const hideShareObject = jest.fn()
    const wrapper = shallow(
      <ShareObjectModal
        object={{ name: "obj1" }}
        shareObjectDetails={{ show: true, object: "obj1", url: "test" }}
        hideShareObject={hideShareObject}
      />
    )
    wrapper
      .find("button")
      .last()
      .simulate("click")
    expect(hideShareObject).toHaveBeenCalled()
  })

  it("should show the shareable link", () => {
    const wrapper = shallow(
      <ShareObjectModal
        object={{ name: "obj1" }}
        shareObjectDetails={{ show: true, object: "obj1", url: "test" }}
      />
    )
    expect(
      wrapper
        .find("input")
        .first()
        .prop("value")
    ).toBe(`${window.location.protocol}//test`)
  })

  it("should call showCopyAlert and hideShareObject when Copy button is clicked", () => {
    const hideShareObject = jest.fn()
    const showCopyAlert = jest.fn()
    const wrapper = shallow(
      <ShareObjectModal
        object={{ name: "obj1" }}
        shareObjectDetails={{ show: true, object: "obj1", url: "test" }}
        hideShareObject={hideShareObject}
        showCopyAlert={showCopyAlert}
      />
    )
    wrapper.find("CopyToClipboard").prop("onCopy")()
    expect(showCopyAlert).toHaveBeenCalledWith("Link copied to clipboard!")
    expect(hideShareObject).toHaveBeenCalled()
  })

  describe("Update expiry values", () => {
    const props = {
      object: { name: "obj1" },
      shareObjectDetails: { show: true, object: "obj1", url: "test" }
    }
    it("should have default expiry values", () => {
      const wrapper = shallow(<ShareObjectModal {...props} />)
      expect(wrapper.state("expiry")).toEqual({
        days: SHARE_OBJECT_EXPIRY_DAYS,
        hours: SHARE_OBJECT_EXPIRY_HOURS,
        minutes: SHARE_OBJECT_EXPIRY_MINUTES
      })
    })

    it("should not allow any increments when days is already max", () => {
      const shareObject = jest.fn()
      const wrapper = shallow(
        <ShareObjectModal {...props} shareObject={shareObject} />
      )
      wrapper.setState({
        expiry: {
          days: 7,
          hours: 0,
          minutes: 0
        }
      })
      wrapper.find("#increase-hours").simulate("click")
      expect(wrapper.state("expiry")).toEqual({
        days: 7,
        hours: 0,
        minutes: 0
      })
      expect(shareObject).not.toHaveBeenCalled()
    })

    it("should not allow expiry values less than minimum value", () => {
      const shareObject = jest.fn()
      const wrapper = shallow(
        <ShareObjectModal {...props} shareObject={shareObject} />
      )
      wrapper.setState({
        expiry: {
          days: 5,
          hours: 0,
          minutes: 0
        }
      })
      wrapper.find("#decrease-hours").simulate("click")
      expect(wrapper.state("expiry").hours).toBe(0)
      wrapper.find("#decrease-minutes").simulate("click")
      expect(wrapper.state("expiry").minutes).toBe(0)
      expect(shareObject).not.toHaveBeenCalled()
    })

    it("should not allow expiry values more than maximum value", () => {
      const shareObject = jest.fn()
      const wrapper = shallow(
        <ShareObjectModal {...props} shareObject={shareObject} />
      )
      wrapper.setState({
        expiry: {
          days: 1,
          hours: 23,
          minutes: 59
        }
      })
      wrapper.find("#increase-hours").simulate("click")
      expect(wrapper.state("expiry").hours).toBe(23)
      wrapper.find("#increase-minutes").simulate("click")
      expect(wrapper.state("expiry").minutes).toBe(59)
      expect(shareObject).not.toHaveBeenCalled()
    })

    it("should set hours and minutes to 0 when days reaches max", () => {
      const shareObject = jest.fn()
      const wrapper = shallow(
        <ShareObjectModal {...props} shareObject={shareObject} />
      )
      wrapper.setState({
        expiry: {
          days: 6,
          hours: 5,
          minutes: 30
        }
      })
      wrapper.find("#increase-days").simulate("click")
      expect(wrapper.state("expiry")).toEqual({
        days: 7,
        hours: 0,
        minutes: 0
      })
      expect(shareObject).toHaveBeenCalled()
    })

    it("should set days to MAX when all of them becomes 0", () => {
      const shareObject = jest.fn()
      const wrapper = shallow(
        <ShareObjectModal {...props} shareObject={shareObject} />
      )
      wrapper.setState({
        expiry: {
          days: 0,
          hours: 1,
          minutes: 0
        }
      })
      wrapper.find("#decrease-hours").simulate("click")
      expect(wrapper.state("expiry")).toEqual({
        days: 7,
        hours: 0,
        minutes: 0
      })
      expect(shareObject).toHaveBeenCalledWith("obj1", 7, 0, 0)
    })
  })
})
