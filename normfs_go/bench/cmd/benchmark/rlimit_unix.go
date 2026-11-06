//go:build unix

package main

import "syscall"

func setupRLimits() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	} else {
		var changed = false
		if rLimit.Cur < 256000 {
			rLimit.Cur = 256000
			changed = true
		}
		if rLimit.Max < 256000 {
			rLimit.Max = 256000
			changed = true
		}
		if !changed {
			return nil
		}

		return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}
}
