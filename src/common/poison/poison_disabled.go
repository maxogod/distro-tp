//go:build !poison

package poison

func ExitIfPoisoned() {}

func DuplicateIfPoisoned() int {
	return 1
}
