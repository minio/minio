// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package jwt

// This file is a re-implementation of the original code here with some
// additional allocation tweaks reproduced using GODEBUG=allocfreetrace=1
// original file https://github.com/golang-jwt/jwt/blob/main/parser.go
// borrowed under MIT License https://github.com/golang-jwt/jwt/blob/main/LICENSE

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

var (
	defaultKeyFunc = func(claim *MapClaims) ([]byte, error) { return []byte("HelloSecret"), nil }
	emptyKeyFunc   = func(claim *MapClaims) ([]byte, error) { return nil, nil }
	errorKeyFunc   = func(claim *MapClaims) ([]byte, error) { return nil, fmt.Errorf("error loading key") }
)

var jwtTestData = []struct {
	name        string
	tokenString string
	keyfunc     func(*MapClaims) ([]byte, error)
	claims      jwt.Claims
	valid       bool
	errors      int32
}{
	{
		"basic",
		"",
		defaultKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
			},
		},
		true,
		0,
	},
	{
		"basic expired",
		"", // autogen
		defaultKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
				"exp": float64(time.Now().Unix() - 100),
			},
		},
		false,
		-1,
	},
	{
		"basic nbf",
		"", // autogen
		defaultKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
				"nbf": float64(time.Now().Unix() + 100),
			},
		},
		false,
		-1,
	},
	{
		"expired and nbf",
		"", // autogen
		defaultKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
				"nbf": float64(time.Now().Unix() + 100),
				"exp": float64(time.Now().Unix() - 100),
			},
		},
		false,
		-1,
	},
	{
		"basic invalid",
		"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIifQ.EhkiHkoESI_cG3NPigFrxEk9Z60_oXrOT2vGm9Pn6RDgYNovYORQmmA0zs1AoAOf09ly2Nx2YAg6ABqAYga1AcMFkJljwxTT5fYphTuqpWdy4BELeSYJx5Ty2gmr8e7RonuUztrdD5WfPqLKMm1Ozp_T6zALpRmwTIW0QPnaBXaQD90FplAg46Iy1UlDKr-Eupy0i5SLch5Q-p2ZpaL_5fnTIUDlxC3pWhJTyx_71qDI-mAA_5lE_VdroOeflG56sSmDxopPEG3bFlSu1eowyBfxtu0_CuVd-M42RU75Zc4Gsj6uV77MBtbMrf4_7M_NUTSgoIF3fRqxrj0NzihIBg",
		defaultKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
			},
		},
		false,
		-1,
	},
	{
		"basic nokeyfunc",
		"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIifQ.FhkiHkoESI_cG3NPigFrxEk9Z60_oXrOT2vGm9Pn6RDgYNovYORQmmA0zs1AoAOf09ly2Nx2YAg6ABqAYga1AcMFkJljwxTT5fYphTuqpWdy4BELeSYJx5Ty2gmr8e7RonuUztrdD5WfPqLKMm1Ozp_T6zALpRmwTIW0QPnaBXaQD90FplAg46Iy1UlDKr-Eupy0i5SLch5Q-p2ZpaL_5fnTIUDlxC3pWhJTyx_71qDI-mAA_5lE_VdroOeflG56sSmDxopPEG3bFlSu1eowyBfxtu0_CuVd-M42RU75Zc4Gsj6uV77MBtbMrf4_7M_NUTSgoIF3fRqxrj0NzihIBg",
		nil,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
			},
		},
		false,
		-1,
	},
	{
		"basic nokey",
		"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIifQ.FhkiHkoESI_cG3NPigFrxEk9Z60_oXrOT2vGm9Pn6RDgYNovYORQmmA0zs1AoAOf09ly2Nx2YAg6ABqAYga1AcMFkJljwxTT5fYphTuqpWdy4BELeSYJx5Ty2gmr8e7RonuUztrdD5WfPqLKMm1Ozp_T6zALpRmwTIW0QPnaBXaQD90FplAg46Iy1UlDKr-Eupy0i5SLch5Q-p2ZpaL_5fnTIUDlxC3pWhJTyx_71qDI-mAA_5lE_VdroOeflG56sSmDxopPEG3bFlSu1eowyBfxtu0_CuVd-M42RU75Zc4Gsj6uV77MBtbMrf4_7M_NUTSgoIF3fRqxrj0NzihIBg",
		emptyKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
			},
		},
		false,
		-1,
	},
	{
		"basic errorkey",
		"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJmb28iOiJiYXIifQ.FhkiHkoESI_cG3NPigFrxEk9Z60_oXrOT2vGm9Pn6RDgYNovYORQmmA0zs1AoAOf09ly2Nx2YAg6ABqAYga1AcMFkJljwxTT5fYphTuqpWdy4BELeSYJx5Ty2gmr8e7RonuUztrdD5WfPqLKMm1Ozp_T6zALpRmwTIW0QPnaBXaQD90FplAg46Iy1UlDKr-Eupy0i5SLch5Q-p2ZpaL_5fnTIUDlxC3pWhJTyx_71qDI-mAA_5lE_VdroOeflG56sSmDxopPEG3bFlSu1eowyBfxtu0_CuVd-M42RU75Zc4Gsj6uV77MBtbMrf4_7M_NUTSgoIF3fRqxrj0NzihIBg",
		errorKeyFunc,
		&MapClaims{
			MapClaims: jwt.MapClaims{
				"foo": "bar",
			},
		},
		false,
		-1,
	},
	{
		"Standard Claims",
		"",
		defaultKeyFunc,
		&StandardClaims{
			StandardClaims: jwt.StandardClaims{
				ExpiresAt: time.Now().Add(time.Second * 10).Unix(),
			},
		},
		true,
		0,
	},
}

func mapClaimsToken(claims *MapClaims) string {
	claims.SetAccessKey("test")
	j := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tk, _ := j.SignedString([]byte("HelloSecret"))
	return tk
}

func standardClaimsToken(claims *StandardClaims) string {
	claims.AccessKey = "test"
	claims.Subject = "test"
	j := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
	tk, _ := j.SignedString([]byte("HelloSecret"))
	return tk
}

func TestParserParse(t *testing.T) {
	// Iterate over test data set and run tests
	for _, data := range jwtTestData {
		t.Run(data.name, func(t *testing.T) {
			// Parse the token
			var err error

			// Figure out correct claims type
			switch claims := data.claims.(type) {
			case *MapClaims:
				if data.tokenString == "" {
					data.tokenString = mapClaimsToken(claims)
				}
				err = ParseWithClaims(data.tokenString, &MapClaims{}, data.keyfunc)
			case *StandardClaims:
				if data.tokenString == "" {
					data.tokenString = standardClaimsToken(claims)
				}
				err = ParseWithStandardClaims(data.tokenString, &StandardClaims{}, []byte("HelloSecret"))
			}

			if data.valid && err != nil {
				t.Errorf("Error while verifying token: %T:%v", err, err)
			}

			if !data.valid && err == nil {
				t.Errorf("Invalid token passed validation")
			}

			if data.errors != 0 {
				_, ok := err.(*jwt.ValidationError)
				if !ok {
					t.Errorf("Expected *jwt.ValidationError, but got %#v instead", err)
				}
			}
		})
	}
}
