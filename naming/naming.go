package naming

import (
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
)

var crc64Table = crc64.MakeTable(crc64.ISO)

// ShortenName deterministically shortens the provided string to the maximum of l characters.
// The function cannot shorten below a length of 8.
// This needs to be deterministic, as we use it to find existing resources.
// ShortenName name will eihter choose CRC64 or CRC32 depending on the requested length l
func ShortenName(s string, l int) (string, error) {
	if len(s) <= l {
		return s, nil
	}
	if l > 32 {
		return ShortenName64(s, l)
	}
	return ShortenName32(s, l)
}

// ShortenName64 deterministically shortens the provided string to the maximum of l characters.
// The function cannot shorten below a length of 16.
// It does this by taking the CRC64 has of the complete string, truncate the name to the first l-16 characters, and appending the hash in hex.
// When using this function for backup pvcs, if we have 100'000 backups of pvcs, that start with the same ~37 letters, that are longer than ~53 letters, and have the same size, in one namespace, the likelihood of a collision, which would cause old backups to be overwritten is less than 1 in 1 Billion.
func ShortenName64(s string, l int) (string, error) {
	if len(s) <= l {
		return s, nil
	}
	if l < 16 {
		return "", errors.New("cannot shorten below 16 characters")
	}
	h := crc64.New(crc64Table)
	h.Write([]byte(s))
	return fmt.Sprintf("%s%16x", s[:l-16], h.Sum64()), nil
}

// ShortenName32 deterministically shortens the provided string to the maximum of l characters.
// The function cannot shorten below a length of 8.
// It does this by taking the CRC32 has of the complete string, truncate the name to the first l-8 characters, and appending the hash in hex.
// When using this function for jobs, if we have 10000 active jobs in one namespace, each copying between pvc that start with the same 19 letters, the likelihood of a collision, which would cause the resize operation to fail is about 1 in 10'000.
// For 1000 active jobs, the likelihood is about 1 in 100'000'000.
func ShortenName32(s string, l int) (string, error) {
	if len(s) <= l {
		return s, nil
	}
	if l < 8 {
		return "", errors.New("cannot shorten below 8 characters")
	}
	h := crc32.NewIEEE()
	h.Write([]byte(s))
	return fmt.Sprintf("%s%08x", s[:l-8], h.Sum32()), nil
}
