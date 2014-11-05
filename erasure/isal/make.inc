########################################################################
#  Copyright(c) 2011-2014 Intel Corporation All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in
#      the documentation and/or other materials provided with the
#      distribution.
#    * Neither the name of Intel Corporation nor the names of its
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
########################################################################


# Makefile include for optimized libraries
#  make targets:
#	lib  - build library of optimized functions
#	slib - build shared library
#	test - run unit tests of functions
#	perf - run performance tests
#	sim  - run on simulator
#	trace - get simulator trace
#	clean - remove object files

CC  = gcc
AS  = yasm
SIM = sde $(SIMFLAGS) --

DEBUG      = -g
DEBUG_yasm = -g dwarf2
DEBUG_nasm = -g

# Default arch= build options
CFLAGS_gcc = -Wall
ASFLAGS_   = -f elf64
ARFLAGS_   = cr $@
STRIP_gcc  = strip -d -R .comment $@
STRIP_clang = strip -d $@

# arch=32 build options
ASFLAGS_32 = -f elf32
CFLAGS_32  = -m32
ARFLAGS_32 = cr $@

# arch=win64 build options
ASFLAGS_win64 = -f win64
CFLAGS_icl    = -Qstd=c99
ARFLAGS_win64 = -out:$@

# arch=mingw build options
ASFLAGS_mingw = -f win64
ARFLAGS_mingw = cr $@
lsrcmingw = $(lsrc)
unit_testsmingw = $(unit_tests)
examplesmingw = $(examples)
perf_testsmingw = $(perf_tests)

ifeq ($(arch),mingw)
  CC=x86_64-w64-mingw32-gcc
  AR=x86_64-w64-mingw32-ar
  LDFLAGS = -Wl,--force-exe-suffix
endif


INCLUDE   = $(patsubst %,-I%,$(subst :, ,$(VPATH)))
CFLAGS   = $(CFLAGS_$(arch)) $(CFLAGS_$(CC)) $(DEBUG) -O2 $(DEFINES) $(INCLUDE)
ASFLAGS  = $(ASFLAGS_$(arch)) $(ASFLAGS_$(CC)) $(DEBUG_$(AS)) $(DEFINES) $(INCLUDE)
ARFLAGS  = $(ARFLAGS_$(arch))
DEFINES += $(addprefix -D , $D)

O = src
lobj  += $(patsubst %.c,%.o,$(patsubst %.asm,%.o,$(lsrc$(arch))))
objs  = $(addprefix $(O)/,$(lobj))


lib_name ?= isa-l.a
default: lib

# Defaults for windows build
ifeq ($(arch),win64)
  AR=lib
  CC=cl
  OUTPUT_OPTION = -Fo$@
  DEBUG=
  lib_name := $(basename $(lib_name)).lib
endif
lsrcwin64 = $(lsrc)
unit_testswin64 = $(unit_tests)
exampleswin64 = $(examples)
perf_testswin64 = $(perf_tests)

# Build and run unit tests, performance tests, etc.
all_tests = $(sort $(perf_tests$(arch)) $(unit_tests$(arch)) $(examples$(arch)) $(other_tests))

$(sort $(unit_tests$(arch))): % : %.c $(tsrc$(arch)) $(lib_name)
$(sort $(perf_tests$(arch))): % : %.c $(lib_name)
$(sort $(examples$(arch))): % : %.c $(lib_name)
$(sort $(other_tests)): % : %.c $(lib_name)

sim test trace: $(addsuffix .run,$(unit_tests$(arch)))
perf: $(addsuffix .run,$(perf_tests$(arch)))
ex:   $(examples$(arch))
all: lib $(all_tests)
other: $(other_tests)
tests: $(unit_tests$(arch))
perfs: $(perf_tests$(arch))
test perf: SIM=
trace: SIMFLAGS = -debugtrace
test sim:
	@echo Finished running tests

$(objs): | $(O)
$(O): ; mkdir -p $(O)


# Build rule to run tests
%.run: %
	$(SIM) $(@D)/$<
	@echo Completed run: $<

# Other build rules
msg = $(if $(DEBUG),DEBUG) $(patsubst 32,32-bit,$(arch)) $D

$(O)/%.o: %.asm
	@echo "  ---> Building $< $(msg)"
	@$(AS) $(ASFLAGS) -o $@ $<

$(O)/%.o %.o: %.c
	@echo "  ---> Building $< $(msg)"
	@$(COMPILE.c) $(OUTPUT_OPTION) $<

$(all_tests):
	@echo "  ---> Building Test $@ $(msg)"
	@$(LINK.o) $(CFLAGS)  $^ $(LDLIBS) -o $@


# Target to build lib files
lib: $(lib_name)
ifneq ($(lib_debug),1)
 $(lib_name): DEBUG_$(AS)=	# Don't put debug symbols in the lib
 $(lib_name): DEBUG=
 $(lib_name): DEFINES+=-D NDEBUG
endif
ifeq ($(lib_debug),1)
 DEBUG+=-D DEBUG   # Define DEBUG for macros
endif

#lib $(lib_name): $(lib_name)(${objs})
$(lib_name): $(objs)
	@echo "  ---> Creating Lib $@"
	@$(AR) $(ARFLAGS) $^
	@$(STRIP_$(CC)) $^

# Target for shared lib
so_lib_name ?= $(basename $(lib_name)).so
slib: $(so_lib_name)
aobjs  += $(addprefix $(O)/,$(patsubst %.asm,%.o,$(filter %.asm,$(lsrc$(arch)))))
shared_objs  += $(addprefix $(O)/shared_ver_,$(patsubst %.c,%.o,$(filter %.c,$(lsrc$(arch)))))

$(O)/shared_ver_%.o: %.c
	@echo "  ---> Building shared $< $(msg)"
	@$(COMPILE.c) $(OUTPUT_OPTION) $<

ifneq ($(lib_debug),1)
 $(so_lib_name): DEBUG_$(AS)=
 $(so_lib_name): DEBUG=
 $(so_lib_name): DEFINES+=-D NDEBUG
endif

$(shared_objs): CFLAGS += -fPIC
$(shared_objs) $(aobjs): | $(O)
$(so_lib_name): $(shared_objs) $(aobjs)
	@echo "  ---> Creating Shared Lib $@"
	@$(CC) $(CFLAGS) -shared  $(LDFLAGS) -o $@ $^
	@$(STRIP_$(CC)) $^

# Collect performance data
rpt_name = perf_report_$(shell uname -n)_$(shell date +%y%m%d).perf

perf_report:
	echo Results for $(rpt_name) >> $(rpt_name)
	$(MAKE) -k perf | tee -a $(rpt_name)
	@echo Summary:
	-grep runtime $(rpt_name)


clean:
	@echo Cleaning up
	@$(RM) -r $(O)/*.o *.a $(all_tests) $(lib_name) $(so_lib_name)
