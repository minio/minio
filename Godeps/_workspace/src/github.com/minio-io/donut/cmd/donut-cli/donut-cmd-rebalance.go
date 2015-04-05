package main

import (
	"log"

	"github.com/minio-io/cli"
	"github.com/minio-io/donut"
)

func doRebalanceDonutCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	donutName := c.Args().First()
	if !isValidDonutName(donutName) {
		log.Fatalln("Invalid donutName")
	}
	mcDonutConfigData, err := loadDonutConfig()
	if err != nil {
		log.Fatalln(err)
	}
	if _, ok := mcDonutConfigData.Donuts[donutName]; !ok {
		log.Fatalln("donut does not exist")
	}
	d, err := donut.NewDonut(donutName, getNodeMap(mcDonutConfigData.Donuts[donutName].Node))
	if err != nil {
		log.Fatalln(err)
	}
	if err := d.Rebalance(); err != nil {
		log.Fatalln(err)
	}
}
