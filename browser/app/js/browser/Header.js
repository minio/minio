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
import ObjectsSearch from "../objects/ObjectsSearch"
import Path from "../objects/Path"
import StorageInfo from "./StorageInfo"
import BrowserDropdown from "./BrowserDropdown"
import web from "../web"
import { minioBrowserPrefix } from "../constants"

export const Header = () => {
  const loggedIn = web.LoggedIn()
  return (
    <header className="fe-header">
      <Path />
      {loggedIn && <StorageInfo />}
      {loggedIn && <ObjectsSearch />}
      <ul className="feh-actions">
        {loggedIn ? (
          <BrowserDropdown />
        ) : (
          <a className="btn btn-danger" href={minioBrowserPrefix + "/login"}>
            Login
          </a>
        )}
      </ul>
    </header>
  )
}

export default Header
