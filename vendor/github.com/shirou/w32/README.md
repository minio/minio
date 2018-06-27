About w32
==========

w32 is a wrapper of windows apis for the Go Programming Language.

It wraps win32 apis to "Go style" to make them easier to use.

Setup
=====

1. Make sure you have a working Go installation and build environment, 
   see this go-nuts post for details:
   http://groups.google.com/group/golang-nuts/msg/5c87630a84f4fd0c
   
   Updated versions of the Windows Go build are available here:
   http://code.google.com/p/gomingw/downloads/list
   
2. Create a "gopath" directory if you do not have one yet and set the
   GOPATH variable accordingly. For example:
   mkdir -p go-externals/src
   export GOPATH=${PWD}/go-externals

3. go get github.com/AllenDang/w32

4. go install github.com/AllenDang/w32...

Contribute
==========

Contributions in form of design, code, documentation, bug reporting or other
ways you see fit are very welcome.

Thank You!
