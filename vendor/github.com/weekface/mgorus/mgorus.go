package mgorus

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type hooker struct {
	c *mgo.Collection
}

type M bson.M

func NewHooker(mgoUrl, db, collection string) (*hooker, error) {
	session, err := mgo.Dial(mgoUrl)
	if err != nil {
		return nil, err
	}

	return &hooker{c: session.DB(db).C(collection)}, nil
}

func (h *hooker) Fire(entry *logrus.Entry) error {
	entry.Data["Level"] = entry.Level.String()
	entry.Data["Time"] = entry.Time
	entry.Data["Message"] = entry.Message
	mgoErr := h.c.Insert(M(entry.Data))
	if mgoErr != nil {
		return fmt.Errorf("Failed to send log entry to mongodb: %s", mgoErr)
	}

	return nil
}

func (h *hooker) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
