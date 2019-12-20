package util

import (
	"fmt"
	"log"
	"strings"
)

func ToUpperCamelCase(pgV string) string {

	ary := strings.Split(pgV, "_")
	for i, v := range ary {
		switch strings.ToLower(v) {
		case "id", "html", "json":
			ary[i] = strings.ToUpper(v)
		default:
			ary[i] = strings.Title(v)
		}
	}
	return strings.Join(ary, "")
}

func ToLowerCamelCase(pgV string) string {

	ary := strings.Split(pgV, "_")
	for i, v := range ary {
		if i == 0 {
			ary[i] = strings.ToLower(v)
		} else {
			switch strings.ToLower(v) {
			case "id", "html", "json":
				ary[i] = strings.ToUpper(v)
			default:
				ary[i] = strings.Title(v)
			}
		}
	}
	return strings.Join(ary, "")
}

func Lpad(s string, l int) string {
	f := fmt.Sprintf("%%-%dv", l)
	return fmt.Sprintf(f, s)
}

func DieOnErrf(s string, err error) {
	if err != nil {
		log.Fatalf(s, err)
	}
}

func DieOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
