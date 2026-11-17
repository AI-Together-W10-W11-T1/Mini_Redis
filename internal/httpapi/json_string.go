package httpapi

import "strconv"

// validStringEscapes checks a raw JSON string after JSON syntax validation.
// Go's decoder replaces unpaired UTF-16 surrogates silently, so inspect the
// original escapes before decoding. A literal U+FFFD is a valid character.
func validStringEscapes(raw []byte) bool {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return false
	}
	for i := 1; i < len(raw)-1; i++ {
		if raw[i] != '\\' {
			continue
		}
		i++ // \\u is literal text; only an unescaped \u starts a code unit.
		if raw[i] != 'u' {
			continue
		}
		unit, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if unit >= 0xdc00 && unit <= 0xdfff {
			return false
		}
		if unit < 0xd800 || unit > 0xdbff {
			continue
		}
		// 상위 서로게이트 바로 뒤에는 하위 서로게이트가 와야 한다.
		if i+6 >= len(raw)-1 || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
