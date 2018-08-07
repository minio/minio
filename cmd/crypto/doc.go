// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package crypto implements AWS S3 related cryptographic building blocks
// for implementing Server-Side-Encryption (SSE-S3) and Server-Side-Encryption
// with customer provided keys (SSE-C).
//
// All objects are encrypted with an unique and randomly generated 'ObjectKey'.
// The ObjectKey itself is never stored in plaintext. Instead it is only stored
// in a sealed from. The sealed 'ObjectKey' is created by encrypting the 'ObjectKey'
// with an unique key-encryption-key. Given the correct key-encryption-key the
// sealed 'ObjectKey' can be unsealed and the object can be decrypted.
//
//
// ## SSE-C
//
// SSE-C computes the key-encryption-key from the client-provided key, an
// initialization vector (IV) and the bucket/object path.
//
// 1. Encrypt:
//       Input: ClientKey, bucket, object, metadata, object_data
//       -              IV := Random({0,1}²⁵⁶)
//       -       ObjectKey := SHA256(ClientKey || Random({0,1}²⁵⁶))
//       -       KeyEncKey := HMAC-SHA256(ClientKey, IV || 'SSE-C' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -       SealedKey := DAREv2_Enc(KeyEncKey, ObjectKey)
//       - enc_object_data := DAREv2_Enc(ObjectKey, object_data)
//       -        metadata <- IV
//       -        metadata <- SealedKey
//       Output: enc_object_data, metadata
//
// 2. Decrypt:
//       Input: ClientKey, bucket, object, metadata, enc_object_data
//       -          IV <- metadata
//       -   SealedKey <- metadata
//       -   KeyEncKey := HMAC-SHA256(ClientKey, IV || 'SSE-C' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -   ObjectKey := DAREv2_Dec(KeyEncKey, SealedKey)
//       - object_data := DAREv2_Dec(ObjectKey, enc_object_data)
//       Output: object_data
//
//
// ## SSE-S3
//
// SSE-S3 can use either a master key or a KMS as root-of-trust.
// The en/decryption slightly depens upon which root-of-trust is used.
//
// ### SSE-S3 and single master key
//
// The master key is used to derive unique object- and key-encryption-keys.
// SSE-S3 with a single master key works as SSE-C where the master key is
// used as the client-provided key.
//
// 1. Encrypt:
//       Input: MasterKey, bucket, object, metadata, object_data
//       -              IV := Random({0,1}²⁵⁶)
//       -       ObjectKey := SHA256(MasterKey || Random({0,1}²⁵⁶))
//       -       KeyEncKey := HMAC-SHA256(MasterKey, IV || 'SSE-S3' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -       SealedKey := DAREv2_Enc(KeyEncKey, ObjectKey)
//       - enc_object_data := DAREv2_Enc(ObjectKey, object_data)
//       -        metadata <- IV
//       -        metadata <- SealedKey
//       Output: enc_object_data, metadata
//
// 2. Decrypt:
//       Input: MasterKey, bucket, object, metadata, enc_object_data
//       -          IV <- metadata
//       -   SealedKey <- metadata
//       -   KeyEncKey := HMAC-SHA256(MasterKey, IV || 'SSE-S3' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -   ObjectKey := DAREv2_Dec(KeyEncKey, SealedKey)
//       - object_data := DAREv2_Dec(ObjectKey, enc_object_data)
//       Output: object_data
//
//
// ### SSE-S3 and KMS
//
// SSE-S3 requires that the KMS provides two functions:
//       1.       Generate(KeyID) -> (Key, EncKey)
//       2. Unseal(KeyID, EncKey) -> Key
//
// 1. Encrypt:
//       Input: KeyID, bucket, object, metadata, object_data
//       -     Key, EncKey := Generate(KeyID)
//       -              IV := Random({0,1}²⁵⁶)
//       -       ObjectKey := SHA256(Key, Random({0,1}²⁵⁶))
//       -       KeyEncKey := HMAC-SHA256(Key, IV || 'SSE-S3' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -       SealedKey := DAREv2_Enc(KeyEncKey, ObjectKey)
//       - enc_object_data := DAREv2_Enc(ObjectKey, object_data)
//       -        metadata <- IV
//       -        metadata <- KeyID
//       -        metadata <- EncKey
//       -        metadata <- SealedKey
//       Output: enc_object_data, metadata
//
// 2. Decrypt:
//       Input: bucket, object, metadata, enc_object_data
//       -      KeyID  <- metadata
//       -      EncKey <- metadata
//       -          IV <- metadata
//       -   SealedKey <- metadata
//       -         Key := Unseal(KeyID, EncKey)
//       -   KeyEncKey := HMAC-SHA256(Key, IV || 'SSE-S3' || 'DAREv2-HMAC-SHA256' || bucket || '/' || object)
//       -   ObjectKey := DAREv2_Dec(KeyEncKey, SealedKey)
//       - object_data := DAREv2_Dec(ObjectKey, enc_object_data)
//       Output: object_data
//
package crypto
