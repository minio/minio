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

import (
	"io/ioutil"
	"log"
)

// Internal levels of library output that are initialised to not print
// anything but can be overridden by programmer
var (
	ERROR    *log.Logger
	CRITICAL *log.Logger
	WARN     *log.Logger
	DEBUG    *log.Logger
)

func init() {
	ERROR = log.New(ioutil.Discard, "", 0)
	CRITICAL = log.New(ioutil.Discard, "", 0)
	WARN = log.New(ioutil.Discard, "", 0)
	DEBUG = log.New(ioutil.Discard, "", 0)
}
