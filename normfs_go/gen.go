//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fmt.Println("Generating protobuf code...")
	cmd := exec.Command(
		"gremlinc",
		"-src", "./../proto",
		"-out", "./pb",
		"-module", "github.com/norma-core/normfs/normfs_go/pb",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to generate protobuf code: %w", err)
	}

	fmt.Println("✓ Protobuf code generated successfully!")
	return nil
}
