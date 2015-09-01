package cors

import (
	"net/http"
	"testing"
)

type FakeResponse struct {
	header http.Header
}

func (r FakeResponse) Header() http.Header {
	return r.header
}

func (r FakeResponse) WriteHeader(n int) {
}

func (r FakeResponse) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func BenchmarkWithout(b *testing.B) {
	res := FakeResponse{http.Header{}}
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testHandler.ServeHTTP(res, req)
	}
}

func BenchmarkDefault(b *testing.B) {
	res := FakeResponse{http.Header{}}
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)
	req.Header.Add("Origin", "somedomain.com")
	handler := Default().Handler(testHandler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(res, req)
	}
}

func BenchmarkAllowedOrigin(b *testing.B) {
	res := FakeResponse{http.Header{}}
	req, _ := http.NewRequest("GET", "http://example.com/foo", nil)
	req.Header.Add("Origin", "somedomain.com")
	c := New(Options{
		AllowedOrigins: []string{"somedomain.com"},
	})
	handler := c.Handler(testHandler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(res, req)
	}
}

func BenchmarkPreflight(b *testing.B) {
	res := FakeResponse{http.Header{}}
	req, _ := http.NewRequest("OPTIONS", "http://example.com/foo", nil)
	req.Header.Add("Access-Control-Request-Method", "GET")
	handler := Default().Handler(testHandler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(res, req)
	}
}

func BenchmarkPreflightHeader(b *testing.B) {
	res := FakeResponse{http.Header{}}
	req, _ := http.NewRequest("OPTIONS", "http://example.com/foo", nil)
	req.Header.Add("Access-Control-Request-Method", "GET")
	req.Header.Add("Access-Control-Request-Headers", "Accept")
	handler := Default().Handler(testHandler)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(res, req)
	}
}
