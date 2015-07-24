package fuzzparse

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/davecgh/go-spew/spew"
)

func init() {
	spew.Config.DisableMethods = true
}

// Fuzz is the entry point for gofuzz.
func Fuzz(data []byte) int {
	sql := string(data)
	stmts, err := parser.Parse(sql)
	if err != nil {
		if stmts != nil {
			panic("stmt is not nil on error")
		}
		return 0
	}
	// TODO(tschottdorf): we're ignoring a lot of things in sql.y, so we end
	// up with nils here.
	if stmts == nil {
		// 	panic("stmt nil on success")
		return 0
	}
	for _, stmt := range stmts {
		_ = fuzzSingle(stmt)
	}
	return 1
}

type nullEnv struct{}

func (nullEnv) Get(_ string) (parser.Datum, bool) {
	return parser.DNull{}, true
}

var env = nullEnv{}

func expected(err error) bool {
	if err == nil {
		return true
	}
	str := err.Error()
	for _, substr := range []string{
		"unsupported",
		"unimplemented",
		"ParseFloat",
		"unknown function",
		"cannot convert",
		"zero modulus",
		"not supported",
		"incorrect number",
		"argument type mismatch",
		`DATABASE`,                    // # 1818
		`syntax error at or near ")"`, // #1817
		"interface is nil, not",       // probably since sql.y ignores unimplemented bits
		`*`, // #1810. Just disencourage * use in general for now.
	} {
		if strings.Contains(str, substr) {
			return true
		}
	}
	return false
}

func fuzzSingle(stmt parser.Statement) int {
	data0 := stmt.String()
	// TODO(tschottdorf): again, this is since we're ignoring stuff in the
	// grammar instead of erroring out on unsupported language.
	if strings.Contains(data0, "%!s(<nil>)") {
		return 0
	}
	stmt1, err := parser.Parse(data0)
	if !expected(err) {
		fmt.Printf("AST: %s", spew.Sdump(stmt))
		fmt.Printf("data0: %q\n", data0)
		panic(err)
	}
	if err != nil {
		return 0
	}
	data1 := stmt1.String()
	// TODO(tschottdorf): due to the ignoring issue again.
	// if !reflect.DeepEqual(stmt, stmt1) {
	if data1 != data0 {
		fmt.Printf("data0: %q\n", data0)
		fmt.Printf("AST: %s", spew.Sdump(stmt))
		fmt.Printf("data1: %q\n", data1)
		fmt.Printf("AST: %s", spew.Sdump(stmt1))
		panic("not equal")
	}

	v := func(e parser.Expr) parser.Expr {
		_, err := parser.EvalExpr(e, env)
		if !expected(err) {
			fmt.Printf("Expr: %s", spew.Sdump(e))
			panic(err)
		}
		return e
	}

	var exprs []parser.Expr
	if sel, ok := stmt.(*parser.Select); ok {
		for _, expr := range sel.Exprs {
			if nsExpr, ok := expr.(*parser.NonStarExpr); ok {
				exprs = append(exprs, nsExpr.Expr)
			}
		}
		for _, w := range [](*parser.Where){sel.Where, sel.Having} {
			if w != nil {
				exprs = append(exprs, w.Expr)
			}
		}
		exprs = append(exprs, sel.GroupBy...)
		for _, order := range sel.OrderBy {
			exprs = append(exprs, order.Expr)
		}
	}
	for _, expr := range exprs {
		parser.WalkExpr(parser.VisitorFunc(v), expr)
	}
	return 1
}
