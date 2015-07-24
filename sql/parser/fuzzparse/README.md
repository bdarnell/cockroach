In this directory, run
```
go-fuzz-build github.com/cockroachdb/cockroach/sql/parser/fuzzparse &&
    go-fuzz -bin=./fuzzparse-fuzz.zip -workdir=.
```
