/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package openid

import (
	"net/http"
	"net/http/httptest"
	"testing"

	xnet "github.com/minio/minio/pkg/net"
)

type errorValidator struct{}

func (e errorValidator) Validate(token, dsecs string) (map[string]interface{}, error) {
	return nil, ErrTokenExpired
}

func (e errorValidator) ID() ID {
	return "err"
}

func TestValidators(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Write([]byte(`{
  "keys" : [ {
    "kty" : "RSA",
    "kid" : "1438289820780",
    "use" : "sig",
    "alg" : "RS256",
    "n" : "idWPro_QiAFOdMsJD163lcDIPogOwXogRo3Pct2MMyeE2GAGqV20Sc8QUbuLDfPl-7Hi9IfFOz--JY6QL5l92eV-GJXkTmidUEooZxIZSp3ghRxLCqlyHeF5LuuM5LPRFDeF4YWFQT_D2eNo_w95g6qYSeOwOwGIfaHa2RMPcQAiM6LX4ot-Z7Po9z0_3ztFa02m3xejEFr2rLRqhFl3FZJaNnwTUk6an6XYsunxMk3Ya3lRaKJReeXeFtfTpShgtPiAl7lIfLJH9h26h2OAlww531DpxHSm1gKXn6bjB0NTC55vJKft4wXoc_0xKZhnWmjQE8d9xE8e1Z3Ll1LYbw",
    "e" : "AQAB"
  }, {
    "kty" : "RSA",
    "kid" : "1438289856256",
    "use" : "sig",
    "alg" : "RS256",
    "n" : "zo5cKcbFECeiH8eGx2D-DsFSpjSKbTVlXD6uL5JAy9rYIv7eYEP6vrKeX-x1z70yEdvgk9xbf9alc8siDfAz3rLCknqlqL7XGVAQL0ZP63UceDmD60LHOzMrx4eR6p49B3rxFfjvX2SWSV3-1H6XNyLk_ALbG6bGCFGuWBQzPJB4LMKCrOFq-6jtRKOKWBXYgkYkaYs5dG-3e2ULbq-y2RdgxYh464y_-MuxDQfvUgP787XKfcXP_XjJZvyuOEANjVyJYZSOyhHUlSGJapQ8ztHdF-swsnf7YkePJ2eR9fynWV2ZoMaXOdidgZtGTa4R1Z4BgH2C0hKJiqRy9fB7Gw",
    "e" : "AQAB"
  } ]
}
`))
		w.(http.Flusher).Flush()
	}))
	defer ts.Close()

	vrs := NewValidators()

	if err := vrs.Add(&errorValidator{}); err != nil {
		t.Fatal(err)
	}

	if err := vrs.Add(&errorValidator{}); err == nil {
		t.Fatal("Unexpected should return error for double inserts")
	}

	if _, err := vrs.Get("unknown"); err == nil {
		t.Fatal("Unexpected should return error for unknown validators")
	}

	v, err := vrs.Get("err")
	if err != nil {
		t.Fatal(err)
	}

	if _, err = v.Validate("", ""); err != ErrTokenExpired {
		t.Fatalf("Expected error %s, got %s", ErrTokenExpired, err)
	}

	vids := vrs.List()
	if len(vids) == 0 || len(vids) > 1 {
		t.Fatalf("Unexpected number of vids %v", vids)
	}

	u, err := xnet.ParseHTTPURL(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{}
	cfg.JWKS.URL = u
	if err = vrs.Add(NewJWT(cfg)); err != nil {
		t.Fatal(err)
	}

	if _, err = vrs.Get("jwt"); err != nil {
		t.Fatal(err)
	}
}
