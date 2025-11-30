//go:build poison

package poison

import (
	"math/rand/v2"
	"os"
)

// ExitIfPoisoned samples a bernoulli to determine if node should
// exit with code 3, or survive.
// (Only if poison is enabled)
func ExitIfPoisoned() {
	sample := rand.Float64()
	if sample > PROBABILITY {
		return
	}
	os.Exit(3) // Exit code for poisoned process
}

// DuplicateIfPoisoned samples a bernoulli to determine if node should
// duplicate a message (return 2) or act normally (return 1).
// (Only if poison is enabled)
func DuplicateIfPoisoned() int {
	sample := rand.Float64()
	if sample > PROBABILITY {
		return 1
	}
	return 2
}
