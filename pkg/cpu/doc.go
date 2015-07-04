// Package cpu provides wrapper around assembly functions for checking processor
// instruction capabilities for SSE4.1, AVX, AVX2 support
//
// Example
//
// ``cpu.HasSSE3()`` returns true for SSE3 instruction support, false otherwise
//
// ``cpu.HasSSE42()`` returns true for SSE4.2 instruction support, false otherwise
//
// ``cpu.HasAVX()`` returns true for AVX instruction support, false otherwise
//
// ``cpu.HasAVX2()`` returns true for AVX2 instruction support, false otherwise
package cpu
