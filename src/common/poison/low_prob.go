//go:build low_prob

package poison

// X: amount of messages until failure ~G(p)
const PROBABILITY = 0.0001 // E[X] = 10000
