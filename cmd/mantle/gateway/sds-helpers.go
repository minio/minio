package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"path"
)

type config struct {
	SdsUrl string `json:"sdsUrl"`
	ApiKey string `json:"apiKey"`
}

var (
	mantleConfig    config
	GATEWAY_ID_LEN  = 24
	SDS_CONFIG_PATH = "SDS_CONFIG_PATH"
)

func init() {
	path := "./cmd/mantle/configMantle/config-mantle.json"
	if p := os.Getenv(SDS_CONFIG_PATH); p != "" {
		path = p
	}

	f, err := os.Open(path)
	defer f.Close()

	if err != nil {
		log.Fatal("Error opening mantle config file. Hint: maybe config-mantle.json is missing?")
	}

	b, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("Error reading mantle config file.")
	}

	err = json.Unmarshal(b, &mantleConfig)
	if err != nil {
		log.Fatal("Error parsing json")
	}

	log.Println("Mantle config file loaded !")
}

func GetId(r io.Reader) (string, error) {
	buf := make([]byte, GATEWAY_ID_LEN)
	c, err := r.Read(buf)
	if c != GATEWAY_ID_LEN {
		return "", errors.New("bad gateway len")
	}

	if err != nil {
		return "", err
	}

	return string(buf[:c]), nil
}

func urlJoin(params ...string) string {
	u, _ := url.Parse(mantleConfig.SdsUrl)

	for _, p := range params {
		u.Path = path.Join(u.Path, p)
	}

	return u.String()
}

func setMantleHeaders(configId string) map[string]string {
	return map[string]string{
		"x-api-key":    mantleConfig.ApiKey,
		"s3-config-id": configId,
	}
}
