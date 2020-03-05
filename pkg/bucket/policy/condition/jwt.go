/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package condition

// JWT claims supported substitutions.
// https://www.iana.org/assignments/jwt/jwt.xhtml#claims
const (
	// JWTSub - JWT subject claim substitution.
	JWTSub Key = "jwt:sub"

	// JWTIss issuer claim substitution.
	JWTIss Key = "jwt:iss"

	// JWTAud audience claim substitution.
	JWTAud Key = "jwt:aud"

	// JWTJti JWT unique identifier claim substitution.
	JWTJti Key = "jwt:jti"

	JWTName         Key = "jwt:name"
	JWTGivenName    Key = "jwt:given_name"
	JWTFamilyName   Key = "jwt:family_name"
	JWTMiddleName   Key = "jwt:middle_name"
	JWTNickName     Key = "jwt:nickname"
	JWTPrefUsername Key = "jwt:preferred_username"
	JWTProfile      Key = "jwt:profile"
	JWTPicture      Key = "jwt:picture"
	JWTWebsite      Key = "jwt:website"
	JWTEmail        Key = "jwt:email"
	JWTGender       Key = "jwt:gender"
	JWTBirthdate    Key = "jwt:birthdate"
	JWTPhoneNumber  Key = "jwt:phone_number"
	JWTAddress      Key = "jwt:address"
	JWTScope        Key = "jwt:scope"
	JWTClientID     Key = "jwt:client_id"
)

// JWTKeys - Supported JWT keys, non-exhaustive list please
// expand as new claims are standardized.
var JWTKeys = []Key{
	JWTSub,
	JWTIss,
	JWTAud,
	JWTJti,
	JWTName,
	JWTGivenName,
	JWTFamilyName,
	JWTMiddleName,
	JWTNickName,
	JWTPrefUsername,
	JWTProfile,
	JWTPicture,
	JWTWebsite,
	JWTEmail,
	JWTGender,
	JWTBirthdate,
	JWTPhoneNumber,
	JWTAddress,
	JWTScope,
	JWTClientID,
}
