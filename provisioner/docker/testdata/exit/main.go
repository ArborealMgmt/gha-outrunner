// Credential-free fixture for the opt-in Docker integration tests.
package main

import (
	"os"
	"time"
)

func main() {
	if len(os.Args) > 2 && os.Args[2] == "hold" {
		time.Sleep(time.Minute)
	}
	os.Exit(17)
}
