//go:build poison

package poison

import (
	"math/rand/v2"
	"os"
)

func ExitIfPoisoned() {
	sample := rand.Float64()
	if sample > PROBABILITY {
		return
	}
	os.Exit(3) // Exit code for poisoned process
}

func DuplicateIfPoisoned() int {
	sample := rand.Float64()
	if sample > PROBABILITY {
		return 1
	}
	return 2
}
