package util

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type LineBuf struct {
	ary []string
}

func InitLineBuf() *LineBuf {
	var a LineBuf
	return &a
}

func (b *LineBuf) Append(s string) {
	b.ary = append(b.ary, s)
}

func WriteFile(dir, filename string, b *LineBuf) {

	f := OpenOutputFile(dir, fmt.Sprintf("%s.go", filename))
	defer FileClose(f)
	w := bufio.NewWriter(f)

	_, err := w.Write([]byte(strings.Join(b.ary, "\n")))
	DieOnErrf("Write failed: %q", err)

	w.Flush()

}

// OpenOutputFile opens the appropriate target for writing output, or dies trying
func OpenOutputFile(dir, filename string) (f *os.File) {

	var err error

	target := filepath.Join(dir, filename)

	if target == "" || target == "-" {
		f = os.Stdout
	} else {
		f, err = os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0644)
		DieOnErrf("File open failed: %q", err)
	}
	return f
}

// FileClose closes a file handle, or dies trying
func FileClose(f *os.File) {
	err := f.Close()
	DieOnErrf("File close failed: %q", err)
}
