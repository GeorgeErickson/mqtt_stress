package utils

import (
	"bytes"
	"encoding/gob"
	"log"
)

func CheckErrFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func DecodeGob(buf *bytes.Buffer, out interface{}) {
	err := gob.NewDecoder(buf).Decode(out)
	CheckErrFatal(err)
}
