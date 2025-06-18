package main

import "errors"

var (
	errInternal = errors.New("an internal error occured")
	errSyntax   = errors.New("syntax error")
)
