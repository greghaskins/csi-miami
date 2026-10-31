package intent

import (
	"fmt"
	"strings"
)

const hexDigits = "0123456789ABCDEF"

func safeByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '.' || c == '_' || c == '-'
}

// Escape makes a volume ID safe as a file name: every byte outside
// [A-Za-z0-9._-] becomes %XX with upper-case hex digits, so
// "directory:pvc-123" becomes "directory%3Apvc-123".
func Escape(id string) string {
	var b strings.Builder
	for i := 0; i < len(id); i++ {
		c := id[i]
		if safeByte(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}
	return b.String()
}

// Unescape is the inverse of Escape. It is for diagnostics only: the volume
// ID inside a record, never the file name, says which volume it is.
func Unescape(name string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if safeByte(c) {
			b.WriteByte(c)
			continue
		}
		if c != '%' || i+2 >= len(name) {
			return "", fmt.Errorf("escaped name %q: unexpected byte at offset %d", name, i)
		}
		hi, ok1 := unhex(name[i+1])
		lo, ok2 := unhex(name[i+2])
		if !ok1 || !ok2 {
			return "", fmt.Errorf("escaped name %q: bad escape at offset %d", name, i)
		}
		b.WriteByte(hi<<4 | lo)
		i += 2
	}
	return b.String(), nil
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}
