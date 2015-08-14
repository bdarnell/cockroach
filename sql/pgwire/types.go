// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package pgwire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/lib/pq/oid"
)

type formatCode int16

const (
	formatText   formatCode = 0
	formatBinary            = 1
)

// TODO(bdarnell): it's not quite this simple, especially when dealing
// with negative years. I can't find authoritative docs but see
// comments in github.com/lib/pq.
const pgTimestampFormat = "2006-01-02 15:04:05.999999999"

// pgType contains type metadata used in RowDescription messages.
type pgType struct {
	oid oid.Oid

	// Variable-size types have size=-1.
	// Note that the protocol has both int16 and int32 size fields,
	// so this attribute is an unsized int and should be cast
	// as needed.
	// This field does *not* correspond to the encoded length of a
	// data type, so it's unclear what, if anything, it is used for.
	// To get the right value, "SELECT oid, typelen FROM pg_types"
	// on a postgres server.
	size int

	// preferredFormat is the one we use when sending this type to the
	// client.
	preferredFormat formatCode
}

func typeForDatum(d driver.Datum) pgType {
	switch d.Payload.(type) {
	case *driver.Datum_BoolVal:
		return pgType{oid.T_bool, 1, formatText}

	case *driver.Datum_IntVal:
		return pgType{oid.T_int8, 8, formatBinary}

	case *driver.Datum_FloatVal:
		return pgType{oid.T_float8, 8, formatText}

	case *driver.Datum_BytesVal, *driver.Datum_StringVal:
		return pgType{oid.T_text, -1, formatText}

	case *driver.Datum_DateVal:
		return pgType{oid.T_date, 8, formatText}

	case *driver.Datum_TimeVal:
		return pgType{oid.T_timestamp, 8, formatText}

	case *driver.Datum_IntervalVal:
		return pgType{oid.T_interval, 8, formatText}

	default:
		panic(fmt.Sprintf("unsupported type %T", d.Payload))
	}
}

func writeDatum(w io.Writer, d driver.Datum) error {
	// NULL is encoded as -1; all other values have a length prefix.
	if d.Payload == nil {
		return binary.Write(w, binary.BigEndian, int32(-1))
	}
	var buf bytes.Buffer
	switch v := d.Payload.(type) {
	case *driver.Datum_BoolVal:
		var b byte
		if v.BoolVal {
			b = 't'
		} else {
			b = 'f'
		}
		if err := buf.WriteByte(b); err != nil {
			return err
		}

	case *driver.Datum_IntVal:
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return err
		}

	case *driver.Datum_FloatVal:
		if _, err := buf.WriteString(strconv.FormatFloat(v.FloatVal, 'f', -1, 64)); err != nil {
			return err
		}

	case *driver.Datum_BytesVal:
		if _, err := buf.Write(v.BytesVal); err != nil {
			return err
		}

	case *driver.Datum_StringVal:
		if _, err := buf.WriteString(v.StringVal); err != nil {
			return err
		}

	case *driver.Datum_DateVal:
		panic(fmt.Sprintf("TODO(pmattis): unsupported type %T", v))

	case *driver.Datum_TimeVal:
		t := v.TimeVal.GoTime()
		if _, err := buf.WriteString(t.Format(pgTimestampFormat)); err != nil {
			return err
		}

	case *driver.Datum_IntervalVal:
		panic(fmt.Sprintf("TODO(pmattis): unsupported type %T", v))

	default:
		panic(fmt.Sprintf("unsupported type %T", v))
	}

	if err := binary.Write(w, binary.BigEndian, int32(buf.Len())); err != nil {
		return err
	}
	_, err := io.Copy(w, &buf)
	return err
}
