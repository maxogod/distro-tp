//go:build poison

package poison

import (
	"math/rand/v2"
	"os"
)

func ExitIfPoisoned() {
	if rand.Float64() < PROBABILITY {
		return
	}
	os.Exit(1)
}

func DuplicateIfPoisoned() int {
	minNum := 1
	maxNum := 4
	return rand.IntN(maxNum) + minNum
}
