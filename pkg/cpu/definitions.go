// Copyright (c) 2015 Klaus Post, released under MIT License. See LICENSE file.

// +build 386 amd64

package cpu

func cpuid(op uint32) (eax, ebx, ecx, edx uint32)
func cpuidex(op, op2 uint32) (eax, ebx, ecx, edx uint32)
func xgetbv(index uint32) (eax, edx uint32)
