//go:build high_prob

package poison

// X: amount of messages until failure ~G(p)
const PROBABILITY = 0.0005 // E[X] = 2000
