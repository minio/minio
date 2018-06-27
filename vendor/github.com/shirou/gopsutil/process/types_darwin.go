// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Hand Writing
// - all pointer in ExternProc to uint64

// +build ignore

/*
Input to cgo -godefs.
*/

// +godefs map struct_in_addr [4]byte /* in_addr */
// +godefs map struct_in6_addr [16]byte /* in6_addr */
// +godefs map struct_ [16]byte /* in6_addr */

package process

/*
#define __DARWIN_UNIX03 0
#define KERNEL
#define _DARWIN_USE_64_BIT_INODE
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <termios.h>
#include <unistd.h>
#include <mach/mach.h>
#include <mach/message.h>
#include <sys/event.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/ptrace.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <net/bpf.h>
#include <net/if_dl.h>
#include <net/if_var.h>
#include <net/route.h>
#include <netinet/in.h>

#include <sys/sysctl.h>
#include <sys/ucred.h>
#include <sys/proc.h>
#include <sys/time.h>
#include <sys/_types/_timeval.h>
#include <sys/appleapiopts.h>
#include <sys/cdefs.h>
#include <sys/param.h>
#include <bsm/audit.h>
#include <sys/queue.h>

enum {
	sizeofPtr = sizeof(void*),
};

union sockaddr_all {
	struct sockaddr s1;	// this one gets used for fields
	struct sockaddr_in s2;	// these pad it out
	struct sockaddr_in6 s3;
	struct sockaddr_un s4;
	struct sockaddr_dl s5;
};

struct sockaddr_any {
	struct sockaddr addr;
	char pad[sizeof(union sockaddr_all) - sizeof(struct sockaddr)];
};

struct ucred_queue {
        struct ucred *tqe_next;
        struct ucred **tqe_prev;
        TRACEBUF
};

*/
import "C"

// Machine characteristics; for internal use.

const (
	sizeofPtr      = C.sizeofPtr
	sizeofShort    = C.sizeof_short
	sizeofInt      = C.sizeof_int
	sizeofLong     = C.sizeof_long
	sizeofLongLong = C.sizeof_longlong
)

// Basic types

type (
	_C_short     C.short
	_C_int       C.int
	_C_long      C.long
	_C_long_long C.longlong
)

// Time

type Timespec C.struct_timespec

type Timeval C.struct_timeval

// Processes

type Rusage C.struct_rusage

type Rlimit C.struct_rlimit

type UGid_t C.gid_t

type KinfoProc C.struct_kinfo_proc

type Eproc C.struct_eproc

type Proc C.struct_proc

type Session C.struct_session

type ucred C.struct_ucred

type Uucred C.struct__ucred

type Upcred C.struct__pcred

type Vmspace C.struct_vmspace

type Sigacts C.struct_sigacts

type ExternProc C.struct_extern_proc

type Itimerval C.struct_itimerval

type Vnode C.struct_vnode

type Pgrp C.struct_pgrp

type UserStruct C.struct_user

type Au_session C.struct_au_session

type Posix_cred C.struct_posix_cred

type Label C.struct_label

type AuditinfoAddr C.struct_auditinfo_addr
type AuMask C.struct_au_mask
type AuTidAddr C.struct_au_tid_addr

// TAILQ(ucred)
type UcredQueue C.struct_ucred_queue
