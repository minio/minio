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
import { getRandomString } from "../utils"
import storage from "local-storage-fallback"
import { buildOpenIDAuthURL, OPEN_ID_NONCE_KEY } from './utils'

export class OpenIDLoginButton extends React.Component {
  constructor(props) {
    super(props)
    this.handleClick = this.handleClick.bind(this)
  }

  handleClick(event) {
    event.stopPropagation()
    const { authEp, authScopes, clientId } = this.props

    let redirectURI = window.location.href.split("#")[0]
    if (redirectURI.endsWith('/')) {
      redirectURI += 'openid'
    } else {
      redirectURI += '/openid'
    }

    // Store nonce in localstorage to check again after the redirect
    const nonce = getRandomString(16)
    storage.setItem(OPEN_ID_NONCE_KEY, nonce)

    const authURL = buildOpenIDAuthURL(authEp, authScopes, redirectURI, clientId, nonce)
    window.location = authURL
  }

  render() {
    const { children, className } = this.props
    return (
      <div onClick={this.handleClick} className={className}>
        {children}
      </div>
    )
  }
}

export default OpenIDLoginButton
