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
