package main

//
// a MapReduce pseudo-application that counts the number of times map/reduce
// tasks are run, to test whether jobs are assigned multiple times even when
// there is no failure.
//
// go build -buildmode=plugin crash.go
//

import "6.824/util"
import "math/rand"
import "strings"
import "strconv"
import "time"
import "fmt"
import "os"
import "io/ioutil"

var count int

func Map(filename string, contents string) []util.KeyValue {
	me := os.Getpid()
	f := fmt.Sprintf("util-worker-jobcount-%d-%d", me, count)
	count++
	err := ioutil.WriteFile(f, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	return []util.KeyValue{util.KeyValue{"a", "x"}}
}

func Reduce(key string, values []string) string {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "util-worker-jobcount") {
			invocations++
		}
	}
	return strconv.Itoa(invocations)
}
