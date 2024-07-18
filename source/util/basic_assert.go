package btree

import (
	"log"
)

// Assert checks if the condition is true, if not, it logs a fatal error.
func assert(condition bool) {
	if !condition {
		log.Fatal("Assertion failed")
	}
}
