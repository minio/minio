/*
 * MinIO Cloud Storage (C) 2019 MinIO, Inc.
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
