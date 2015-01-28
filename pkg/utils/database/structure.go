package database

import (
	"github.com/HouzuoGuo/tiedot/db"
)

type Database struct {
	DBdir    string
	DBhandle *db.DB
}
