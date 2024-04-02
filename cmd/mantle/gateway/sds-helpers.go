package gateway

import (
	"encoding/json"
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
	mantleConfig config
)

func init() {
	f, err := os.Open("./cmd/mantle/configMantle/config-mantle.json")
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
