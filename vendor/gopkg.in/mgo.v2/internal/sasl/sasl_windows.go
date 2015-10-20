package sasl

// #include "sasl_windows.h"
import "C"

import (
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

type saslStepper interface {
	Step(serverData []byte) (clientData []byte, done bool, err error)
	Close()
}

type saslSession struct {
	// Credentials
	mech          string
	service       string
	host          string
	userPlusRealm string
	target        string
	domain        string

	// Internal state
	authComplete bool
	errored      bool
	step         int

	// C internal state
	credHandle C.CredHandle
	context    C.CtxtHandle
	hasContext C.int

	// Keep track of pointers we need to explicitly free
	stringsToFree []*C.char
}

var initError error
var initOnce sync.Once

func initSSPI() {
	rc := C.load_secur32_dll()
	if rc != 0 {
		initError = fmt.Errorf("Error loading libraries: %v", rc)
	}
}

func New(username, password, mechanism, service, host string) (saslStepper, error) {
	initOnce.Do(initSSPI)
	ss := &saslSession{mech: mechanism, hasContext: 0, userPlusRealm: username}
	if service == "" {
		service = "mongodb"
	}
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}
	ss.service = service
	ss.host = host

	usernameComponents := strings.Split(username, "@")
	if len(usernameComponents) < 2 {
		return nil, fmt.Errorf("Username '%v' doesn't contain a realm!", username)
	}
	user := usernameComponents[0]
	ss.domain = usernameComponents[1]
	ss.target = fmt.Sprintf("%s/%s", ss.service, ss.host)

	var status C.SECURITY_STATUS
	// Step 0: call AcquireCredentialsHandle to get a nice SSPI CredHandle
	if len(password) > 0 {
		status = C.sspi_acquire_credentials_handle(&ss.credHandle, ss.cstr(user), ss.cstr(password), ss.cstr(ss.domain))
	} else {
		status = C.sspi_acquire_credentials_handle(&ss.credHandle, ss.cstr(user), nil, ss.cstr(ss.domain))
	}
	if status != C.SEC_E_OK {
		ss.errored = true
		return nil, fmt.Errorf("Couldn't create new SSPI client, error code %v", status)
	}
	return ss, nil
}

func (ss *saslSession) cstr(s string) *C.char {
	cstr := C.CString(s)
	ss.stringsToFree = append(ss.stringsToFree, cstr)
	return cstr
}

func (ss *saslSession) Close() {
	for _, cstr := range ss.stringsToFree {
		C.free(unsafe.Pointer(cstr))
	}
}

func (ss *saslSession) Step(serverData []byte) (clientData []byte, done bool, err error) {
	ss.step++
	if ss.step > 10 {
		return nil, false, fmt.Errorf("too many SSPI steps without authentication")
	}
	var buffer C.PVOID
	var bufferLength C.ULONG
	if len(serverData) > 0 {
		buffer = (C.PVOID)(unsafe.Pointer(&serverData[0]))
		bufferLength = C.ULONG(len(serverData))
	}
	var status C.int
	if ss.authComplete {
		// Step 3: last bit of magic to use the correct server credentials
		status = C.sspi_send_client_authz_id(&ss.context, &buffer, &bufferLength, ss.cstr(ss.userPlusRealm))
	} else {
		// Step 1 + Step 2: set up security context with the server and TGT
		status = C.sspi_step(&ss.credHandle, ss.hasContext, &ss.context, &buffer, &bufferLength, ss.cstr(ss.target))
	}
	if buffer != C.PVOID(nil) {
		defer C.free(unsafe.Pointer(buffer))
	}
	if status != C.SEC_E_OK && status != C.SEC_I_CONTINUE_NEEDED {
		ss.errored = true
		return nil, false, ss.handleSSPIErrorCode(status)
	}

	clientData = C.GoBytes(unsafe.Pointer(buffer), C.int(bufferLength))
	if status == C.SEC_E_OK {
		ss.authComplete = true
		return clientData, true, nil
	} else {
		ss.hasContext = 1
		return clientData, false, nil
	}
}

func (ss *saslSession) handleSSPIErrorCode(code C.int) error {
	switch {
	case code == C.SEC_E_TARGET_UNKNOWN:
		return fmt.Errorf("Target %v@%v not found", ss.target, ss.domain)
	}
	return fmt.Errorf("Unknown error doing step %v, error code %v", ss.step, code)
}
