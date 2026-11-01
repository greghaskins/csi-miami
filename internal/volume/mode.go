package volume

// DefaultMode is the mode of a directory item the driver creates when the
// StorageClass sets none.
const DefaultMode uint32 = 0o755

// ParseMode parses the value of the mode parameter: an octal number of one to
// four digits (0755, 0777, 2775, 755), the permission bits with the optional
// setuid, setgid and sticky bits in the leading digit and no other bit. It
// returns the mode and its canonical text, always four digits (0755), which is
// what the volume context carries. Anything else (a sign, a prefix such as
// 0o, a digit above 7, more than four digits, a space) is InvalidArgument.
func ParseMode(s string) (mode uint32, canonical string, err error) {
	if s == "" || len(s) > 4 {
		return 0, "", invalid("parameter " + ParamMode + " must be an octal mode of at most four digits, such as 0755")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '7' {
			return 0, "", invalid("parameter " + ParamMode + " must be an octal mode of at most four digits, such as 0755")
		}
		mode = mode<<3 | uint32(c-'0')
	}
	return mode, formatMode(mode), nil
}

// formatMode renders a mode as four octal digits.
func formatMode(mode uint32) string {
	const digits = "01234567"
	b := [4]byte{}
	for i := 3; i >= 0; i-- {
		b[i] = digits[mode&7]
		mode >>= 3
	}
	return string(b[:])
}

// CheckMode reports whether s is empty (the default) or the canonical text of
// a mode, exactly as ParseMode returns it. The error is InvalidArgument.
func CheckMode(s string) error {
	if s == "" {
		return nil
	}
	_, canonical, err := ParseMode(s)
	if err != nil {
		return err
	}
	if canonical != s {
		return invalid("mode must be four octal digits, such as 0755")
	}
	return nil
}

// ModeBits returns the mode a Context or intent carries: DefaultMode for the
// empty string, otherwise the parsed value. A text CheckMode would refuse
// yields DefaultMode; callers check first.
func ModeBits(s string) uint32 {
	if s == "" {
		return DefaultMode
	}
	mode, _, err := ParseMode(s)
	if err != nil {
		return DefaultMode
	}
	return mode
}
