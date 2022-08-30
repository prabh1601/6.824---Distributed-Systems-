package util

import (
	"log"
	"plugin"
)

type KeyValue struct {
	Key   string
	Value string
}

type Pair[U, V any] struct {
	First  U
	Second V
}

func CheckError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Printf(format, v)
	}
}

func LoadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	CheckError(err, "cannot load plugin %s", filename)

	xmapf, err := p.Lookup("Map")
	CheckError(err, "cannot find Map in %s", filename)
	mapf := xmapf.(func(string, string) []KeyValue)

	xreducef, err := p.Lookup("Reduce")
	CheckError(err, "cannot find Reduce in %s", filename)
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
