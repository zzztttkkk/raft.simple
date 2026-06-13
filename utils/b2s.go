package utils

import "unsafe"

func StringView(sv string) []byte {
	return unsafe.Slice(unsafe.StringData(sv), len(sv))
}
