package database

import (
	"github.com/HouzuoGuo/tiedot/db"
)

func NewDatabase() *Database {
	return &Database{}
}

func (data *Database) setdbdir(dirname string) {
	data.DBdir = dirname
}

func (data *Database) GetDBHandle(dirname string) error {
	var err error
	data.setdbdir(dirname)
	data.DBhandle, err = db.OpenDB(data.DBdir)
	if err != nil {
		return err
	}
	return nil
}

func (data *Database) InitCollection(colls ...string) {
	for _, str := range colls {
		data.DBhandle.Create(str)
	}
}

func (data *Database) GetCollections() []string {
	var colls []string
	for _, name := range data.DBhandle.AllCols() {
		colls = append(colls, name)
	}
	return colls
}

func (data *Database) getCollectionHandle(coll string) *db.Col {
	return data.DBhandle.Use(coll)
}

func (data *Database) GetCollectionData(coll string, docid int) (map[string]interface{}, error) {
	collHandle := data.getCollectionHandle(coll)
	return collHandle.Read(docid)
}

func (data *Database) InsertToCollection(coll string, model map[string]interface{}) (docid int, err error) {
	collHandle := data.getCollectionHandle(coll)
	return collHandle.Insert(model)
}
