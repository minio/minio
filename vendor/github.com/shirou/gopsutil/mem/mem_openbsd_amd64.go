// Created by cgo -godefs - DO NOT EDIT
// cgo -godefs types_openbsd.go

package mem

const (
	CTLVm         = 2
	CTLVfs        = 10
	VmUvmexp      = 4
	VfsGeneric    = 0
	VfsBcacheStat = 3
)

const (
	sizeOfUvmexp      = 0x154
	sizeOfBcachestats = 0x78
)

type Uvmexp struct {
	Pagesize           int32
	Pagemask           int32
	Pageshift          int32
	Npages             int32
	Free               int32
	Active             int32
	Inactive           int32
	Paging             int32
	Wired              int32
	Zeropages          int32
	Reserve_pagedaemon int32
	Reserve_kernel     int32
	Anonpages          int32
	Vnodepages         int32
	Vtextpages         int32
	Freemin            int32
	Freetarg           int32
	Inactarg           int32
	Wiredmax           int32
	Anonmin            int32
	Vtextmin           int32
	Vnodemin           int32
	Anonminpct         int32
	Vtextminpct        int32
	Vnodeminpct        int32
	Nswapdev           int32
	Swpages            int32
	Swpginuse          int32
	Swpgonly           int32
	Nswget             int32
	Nanon              int32
	Nanonneeded        int32
	Nfreeanon          int32
	Faults             int32
	Traps              int32
	Intrs              int32
	Swtch              int32
	Softs              int32
	Syscalls           int32
	Pageins            int32
	Obsolete_swapins   int32
	Obsolete_swapouts  int32
	Pgswapin           int32
	Pgswapout          int32
	Forks              int32
	Forks_ppwait       int32
	Forks_sharevm      int32
	Pga_zerohit        int32
	Pga_zeromiss       int32
	Zeroaborts         int32
	Fltnoram           int32
	Fltnoanon          int32
	Fltpgwait          int32
	Fltpgrele          int32
	Fltrelck           int32
	Fltrelckok         int32
	Fltanget           int32
	Fltanretry         int32
	Fltamcopy          int32
	Fltnamap           int32
	Fltnomap           int32
	Fltlget            int32
	Fltget             int32
	Flt_anon           int32
	Flt_acow           int32
	Flt_obj            int32
	Flt_prcopy         int32
	Flt_przero         int32
	Pdwoke             int32
	Pdrevs             int32
	Pdswout            int32
	Pdfreed            int32
	Pdscans            int32
	Pdanscan           int32
	Pdobscan           int32
	Pdreact            int32
	Pdbusy             int32
	Pdpageouts         int32
	Pdpending          int32
	Pddeact            int32
	Pdreanon           int32
	Pdrevnode          int32
	Pdrevtext          int32
	Fpswtch            int32
	Kmapent            int32
}
type Bcachestats struct {
	Numbufs       int64
	Numbufpages   int64
	Numdirtypages int64
	Numcleanpages int64
	Pendingwrites int64
	Pendingreads  int64
	Numwrites     int64
	Numreads      int64
	Cachehits     int64
	Busymapped    int64
	Dmapages      int64
	Highpages     int64
	Delwribufs    int64
	Kvaslots      int64
	Avail         int64
}
