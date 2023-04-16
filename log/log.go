package log

import "fmt"

var LEVEL int

const RESULTS = 1
const INFO = 2
const DEBUG = 3
const TRACE = 4

func init() {
	LEVEL = 1
}

func Debug(msg string, values ...interface{}) {
	LogAtLevel(msg, DEBUG, values...)
}

func Trace(msg string, values ...interface{}) {
	LogAtLevel(msg, TRACE, values...)
}

func LogAtLevel(msg string, level int, values ...interface{}) {
	if LEVEL >= level {
		fmt.Printf(msg, values...)
	}
}
