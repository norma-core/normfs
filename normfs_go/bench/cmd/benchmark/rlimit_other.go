//go:build !unix

package main

// Windows has no descriptor limit to raise.
func setupRLimits() error {
	return nil
}
