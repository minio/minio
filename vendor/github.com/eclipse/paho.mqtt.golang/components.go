/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

type component string

// Component names for debug output
const (
	NET component = "[net]     "
	PNG component = "[pinger]  "
	CLI component = "[client]  "
	DEC component = "[decode]  "
	MES component = "[message] "
	STR component = "[store]   "
	MID component = "[msgids]  "
	TST component = "[test]    "
	STA component = "[state]   "
	ERR component = "[error]   "
)
