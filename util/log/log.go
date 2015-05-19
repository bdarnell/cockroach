// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Brad Seiler (cockroach@bradseiler.com)

package log

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/net/context"
)

const humanTimeFormat = "1/2 15:04:05.00" // m/d h:m:s.ss
const machineTimeFormat = time.RFC3339

// humanLogging is controlled via pflags. If true, we want to print pretty
// information for humans to stderr.
var humanLogging bool

func init() {
	// TODO(tschottdorf) this should go to our logger. Currently this will log
	// with clog (=glog) format.
	CopyStandardLogTo("INFO")
}

// FatalOnPanic recovers from a panic and exits the process with a
// Fatal log. This is useful for avoiding a panic being caught through
// a CGo exported function or preventing HTTP handlers from recovering
// panics and ignoring them.
func FatalOnPanic() {
	if r := recover(); r != nil {
		Fatalf("unexpected panic: %s", r)
	}
}

func printStructured(buf *bytes.Buffer, kvs ...[]interface{}) {
	var i int
	var s string
	var kv []interface{}
	for kvi := range kvs {
		kv = kvs[kvi]
		l := len(kv)
		if l%2 != 0 {
			// TODO(tschottdorf,mrtracy): Consider just making the best of it
			// instead of panicking. A rare code path with an error like this
			// could panic an otherwise working system.
			panic(fmt.Sprintf("odd number of key-value pairs passed: %v", kv))
		}

		for i = 0; i < l; i++ {
			if i%2 == 0 {
				if kvi != 0 || i != 0 {
					buf.WriteString(" ")
				}
			} else {
				buf.WriteString("=")
			}
			switch kt := kv[i].(type) {
			case string:
				s = kt
			case fmt.Stringer:
				s = kt.String()
			default:
				s = fmt.Sprintf("%v", kt)
			}
			if i%2 == 0 {
				buf.WriteString(s)
			} else {
				buf.WriteString(strconv.Quote(s))
			}
		}
	}
	buf.WriteString("\n")
}

func headerKV(sev severity, depth int, msg string) []interface{} {
	file, line := Caller(depth + 1)
	return []interface{}{
		"L", severityName[sev],
		"T", time.Now().Format(machineTimeFormat),
		"F", file + ":" + strconv.Itoa(line),
		"Msg", msg,
	}
}

func printHuman(buf *bytes.Buffer, kvs ...[]interface{}) {
	// TODO(tschottdorf): implement this next.
	_, _ = buf.WriteString("unimplemented")
}

func logDepth(ctx context.Context, depth int, sev severity, msg string, kvs []interface{}) {
	// TODO(tschottdorf): logging hooks should have their entry point here.
	hKV := headerKV(sev, depth+1, msg) // be careful moving this around (depth).
	cKV := contextKV(ctx)
	PrintWith(sev, depth+1, func(buf *bytes.Buffer) {
		if humanLogging {
			printHuman(buf, hKV, cKV, kvs)
		} else {
			printStructured(buf, hKV, cKV, kvs)
		}
	})
}

// Infoc logs to the WARNING and INFO logs. It extracts values from the context
// using the Field keys specified in this package and logs them along with the
// given message and any additional pairs specified as consecutive elements in
// kvs.
func Infoc(ctx context.Context, msg string, kvs ...interface{}) {
	logDepth(ctx, 1, infoLog, msg, kvs)
}

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Info(args ...interface{}) {
	logDepth(nil, 1, infoLog, fmt.Sprint(args...), nil)
}

// Infof logs to the INFO log. Don't use it; use Info or Infoc instead.
// Arguments are handled in the manner of fmt.Printf; a newline is appended if missing.
func Infof(format string, args ...interface{}) {
	logDepth(nil, 1, infoLog, fmt.Sprintf(format, args...), nil)
}

// InfoDepth logs to the INFO log, offsetting the caller's stack frame by
// 'depth'.
func InfoDepth(depth int, args ...interface{}) {
	logDepth(nil, depth+1, infoLog, fmt.Sprint(args...), nil)
}

// Warningc logs to the WARNING and INFO logs. It extracts values from the
// context using the Field keys specified in this package and logs them along
// with the given message and any additional pairs specified as consecutive
// elements in kvs.
func Warningc(ctx context.Context, msg string, kvs ...interface{}) {
	logDepth(ctx, 1, warningLog, msg, kvs)
}

// Warning logs to the WARNING and INFO logs.
// Warningf logs to the WARNING and INFO logs. Don't use it; use Warning or
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Warning(args ...interface{}) {
	logDepth(nil, 1, warningLog, fmt.Sprint(args...), nil)
}

// Warningf logs to the WARNING and INFO logs. Don't use it; use Warning or
// Warningc instead. Arguments are handled in the manner of fmt.Printf; a
// newline is appended if missing.
func Warningf(format string, args ...interface{}) {
	logDepth(nil, 1, warningLog, fmt.Sprintf(format, args...), nil)
}

// WarningDepth logs to the WARNING and INFO logs, offsetting the caller's
// stack frame by 'depth'.
func WarningDepth(depth int, args ...interface{}) {
	logDepth(nil, depth+1, warningLog, fmt.Sprint(args...), nil)
}

// Errorc logs to the ERROR, WARNING, and INFO logs. It extracts values from
// Field keys specified in this package and logs them along with the given
// message and any additional pairs specified as consecutive elements in kvs.
func Errorc(ctx context.Context, msg string, kvs ...interface{}) {
	logDepth(ctx, 1, errorLog, msg, kvs)
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Error(args ...interface{}) {
	logDepth(nil, 1, errorLog, fmt.Sprint(args...), nil)
}

// Errorf logs to the ERROR, WARNING, and INFO logs. Don't use it; use Error
// Info or Errorc instead. Arguments are handled in the manner of fmt.Printf;
// a newline is appended if missing.
func Errorf(format string, args ...interface{}) {
	logDepth(nil, 1, errorLog, fmt.Sprintf(format, args...), nil)
}

// ErrorDepth logs to the ERROR, WARNING, and INFO logs, offsetting the
// caller's stack frame by 'depth'.
func ErrorDepth(depth int, args ...interface{}) {
	logDepth(nil, depth+1, errorLog, fmt.Sprint(args...), nil)
}

// Fatalc logs to the INFO, WARNING, ERROR, and FATAL logs, including a stack
// trace of all running goroutines, then calls os.Exit(255). It extracts values
// from the context using the Field keys specified in this package and logs
// them along with the given message and any additional pairs specified as
// consecutive elements in kvs.
func Fatalc(ctx context.Context, msg string, kvs ...interface{}) {
	logDepth(ctx, 1, fatalLog, msg, kvs)
}

// Fatal logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Print; a newline is appended.
func Fatal(args ...interface{}) {
	logDepth(nil, 1, fatalLog, fmt.Sprint(args...), nil)
}

// Fatalf logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255).
// Arguments are handled in the manner of fmt.Printf; a newline is appended.
func Fatalf(format string, args ...interface{}) {
	logDepth(nil, 1, fatalLog, fmt.Sprintf(format, args...), nil)
}

// FatalDepth logs to the INFO, WARNING, ERROR, and FATAL logs,
// including a stack trace of all running goroutines, then calls os.Exit(255),
// offsetting the caller's stack frame by 'depth'.
func FatalDepth(depth int, args ...interface{}) {
	logDepth(nil, depth+1, fatalLog, fmt.Sprint(args...), nil)
}

// V returns true if the logging verbosity is set to the specified level or
// higher.
func V(level level) bool {
	return VDepth(level, 1)
}
