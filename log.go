package main

import "fmt"

var LEVEL int

const RESULTS = 1
const INFO = 2
const DEBUG = 3
const TRACE = 4

func Debug(msg string, values ...interface{}) {
	LogAtLevel(msg, DEBUG, values...)
}

func Trace(msg string, values ...interface{}) {
	LogAtLevel(msg, TRACE, values...)
}

func LogAtLevel(msg string, level int, values ...interface{}) {
	if LEVEL == 0 {
		LEVEL = RESULTS
	}
	if LEVEL >= level {
		fmt.Printf(msg, values...)
	}
}
