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

var p = window.location.pathname
export const minioBrowserPrefix = p.slice(0, p.indexOf("/", 1))

export const READ_ONLY = "readonly"
export const WRITE_ONLY = "writeonly"
export const READ_WRITE = "readwrite"
export const NONE = "none"

export const SHARE_OBJECT_EXPIRY_DAYS = 5
export const SHARE_OBJECT_EXPIRY_HOURS = 0
export const SHARE_OBJECT_EXPIRY_MINUTES = 0

export const ACCESS_KEY_MIN_LENGTH = 3
export const SECRET_KEY_MIN_LENGTH = 8

export const SORT_BY_NAME = "name"
export const SORT_BY_SIZE = "size"
export const SORT_BY_LAST_MODIFIED = "last-modified"

export const SORT_ORDER_ASC = "asc"
export const SORT_ORDER_DESC = "desc"
