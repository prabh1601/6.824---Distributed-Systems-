package main

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin nocrash.go
//

import "6.824/util"
import crand "crypto/rand"
import "math/big"
import "strings"
import "os"
import "sort"
import "strconv"

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if false && rr.Int64() < 500 {
		// crash!
		os.Exit(1)
	}
}

func Map(filename string, contents string) []util.KeyValue {
	maybeCrash()

	kva := []util.KeyValue{}
	kva = append(kva, util.KeyValue{"a", filename})
	kva = append(kva, util.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, util.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, util.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
