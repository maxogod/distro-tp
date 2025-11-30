//go:build !poison

package poison

// ExitIfPoisoned samples a bernoulli to determine if node should
// exit with code 3, or survive.
// (Only if poison is enabled)
func ExitIfPoisoned() {}

// DuplicateIfPoisoned samples a bernoulli to determine if node should
// duplicate a message (return 2) or act normally (return 1).
// (Only if poison is enabled)
func DuplicateIfPoisoned() int {
	return 1
}
