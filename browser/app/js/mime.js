/*
 * MinIO Cloud Storage (C) 2016 MinIO, Inc.
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

import mimedb from "mime-types"

const isFolder = (name, contentType) => {
  if (name.endsWith("/")) return true
  return false
}

const isPdf = (name, contentType) => {
  if (contentType === "application/pdf") return true
  return false
}
const isImage = (name, contentType) => {
  if (
    contentType === "image/jpeg" ||
    contentType === "image/gif" ||
    contentType === "image/x-icon" ||
    contentType === "image/png" ||
    contentType === "image/svg+xml" ||
    contentType === "image/tiff" ||
    contentType === "image/webp"
  )
    return true
  return false
}

const isZip = (name, contentType) => {
  if (!contentType || !contentType.includes("/")) return false
  if (contentType.split("/")[1].includes("zip")) return true
  return false
}

const isCode = (name, contentType) => {
  const codeExt = [
    "c",
    "cpp",
    "go",
    "py",
    "java",
    "rb",
    "js",
    "pl",
    "fs",
    "php",
    "css",
    "less",
    "scss",
    "coffee",
    "net",
    "html",
    "rs",
    "exs",
    "scala",
    "hs",
    "clj",
    "el",
    "scm",
    "lisp",
    "asp",
    "aspx",
  ]
  const ext = name.split(".").reverse()[0]
  for (var i in codeExt) {
    if (ext === codeExt[i]) return true
  }
  return false
}

const isExcel = (name, contentType) => {
  if (!contentType || !contentType.includes("/")) return false
  const types = ["excel", "spreadsheet"]
  const subType = contentType.split("/")[1]
  for (var i in types) {
    if (subType.includes(types[i])) return true
  }
  return false
}

const isDoc = (name, contentType) => {
  if (!contentType || !contentType.includes("/")) return false
  const types = ["word", ".document"]
  const subType = contentType.split("/")[1]
  for (var i in types) {
    if (subType.includes(types[i])) return true
  }
  return false
}

const isPresentation = (name, contentType) => {
  if (!contentType || !contentType.includes("/")) return false
  var types = ["powerpoint", "presentation"]
  const subType = contentType.split("/")[1]
  for (var i in types) {
    if (subType.includes(types[i])) return true
  }
  return false
}

const typeToIcon = (type) => {
  return (name, contentType) => {
    if (!contentType || !contentType.includes("/")) return false
    if (contentType.split("/")[0] === type) return true
    return false
  }
}

export const getDataType = (name, contentType) => {
  if (contentType === "") {
    contentType = mimedb.lookup(name) || "application/octet-stream"
  }
  const check = [
    ["folder", isFolder],
    ["code", isCode],
    ["audio", typeToIcon("audio")],
    ["image", typeToIcon("image")],
    ["video", typeToIcon("video")],
    ["text", typeToIcon("text")],
    ["pdf", isPdf],
    ["image", isImage],
    ["zip", isZip],
    ["excel", isExcel],
    ["doc", isDoc],
    ["presentation", isPresentation],
  ]
  for (var i in check) {
    if (check[i][1](name, contentType)) return check[i][0]
  }
  return "other"
}
