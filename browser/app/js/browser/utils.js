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

export const OPEN_ID_NONCE_KEY = 'openIDKey'

export const buildOpenIDAuthURL = (authEp, authScopes, redirectURI, clientID, nonce) => {
  const params = new URLSearchParams()
  params.set("response_type", "id_token")
  params.set("scope", authScopes.join(" "))
  params.set("client_id", clientID)
  params.set("redirect_uri", redirectURI)
  params.set("nonce", nonce)

  return `${authEp}?${params.toString()}`
}
