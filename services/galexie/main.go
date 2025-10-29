package main

import (
	"fmt"
	"os"

	galexie "github.com/stellar/stellar-galexie/services/galexie/internal"
)

func main() {
	err := galexie.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
