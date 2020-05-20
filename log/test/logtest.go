// Copyright 2018-2020 Celer Network

// Try run with different flags. Example:
// go run logtest.go -logcolor -loglevel=debug
// go run logtest.go -loglevel=warn -loglocaltime -loglongfile
// go run logtest.go -testoutput

package main

import (
	"flag"
	"fmt"

	"github.com/celer-network/goutils/log"
)

type TestOutput struct {
}

func (cb TestOutput) Write(output []byte) (n int, err error) {
	fmt.Printf("receive log output: %s", output)
	return len(output), nil
}

var testoutput = flag.Bool("testoutput", false, "test log output callback")

func main() {
	flag.Parse()
	if *testoutput {
		log.SetOutput(TestOutput{})
	}
	log.Trace("trace every step")
	log.Debug("looking into what's really happening")
	log.Infof("x is set to %d", 2)
	log.Warnln("watch out!", "enemy is coming!")
	log.Error("something is wrong")
	log.Fatal("get me out of here!")
}
