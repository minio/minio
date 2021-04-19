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
        shareObjectDetails={{ show: true, object: "obj1", url: "test", showExpiryDate: true }}
      />
    )
  })

  it("shoud call hideShareObject when Cancel is clicked", () => {
    const hideShareObject = jest.fn()
    const wrapper = shallow(
      <ShareObjectModal
        object={{ name: "obj1" }}
        shareObjectDetails={{ show: true, object: "obj1", url: "test", showExpiryDate: true }}
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
        shareObjectDetails={{ show: true, object: "obj1", url: "test", showExpiryDate: true }}
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
        shareObjectDetails={{ show: true, object: "obj1", url: "test", showExpiryDate: true }}
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
      shareObjectDetails: { show: true, object: "obj1", url: "test", showExpiryDate: true }
    }

    it("should not show expiry values if shared with public link", () => {
      const shareObjectDetails = { show: true, object: "obj1", url: "test", showExpiryDate: false }
      const wrapper = shallow(<ShareObjectModal {...props} shareObjectDetails={shareObjectDetails} />)
      expect(wrapper.find('.set-expire').exists()).toEqual(false)
    })

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
