package naming

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// some longer names to test on
var names = [...]string{
	"foo",
	"foo-bar-buzz",
	"foo-bar-buzz-124155161",
	"foo-bar-buzz-12415516115215125125521",
	"foo-bar-buzz-12415516115215125125215251525152152125517726657678798228821",
	"foo-bar-buzz-12415516115215125125215251525152152125517726657678798228821-test",
	"foo-bar-buzz-12415516115215125125215251525152152125517726657678798228821-backup",
	"foo-bar-buzz-12415516115215125125215251525152152125517726657678798228821-backup2",
	"foo-bar-buzz-12415516115215125125215251525152152125517726157678798228821-backup2",
	"foo-bar-buzz-12415516115215125125215251525152152125517726157678798228821-tets-2",
	"bar-foo-buzz",
	"bar-foo-buzz-12456",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582018551009",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582018551009-new",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582018551009-test",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582018551009-test2",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582118551009-test2",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582118551009-test-backup",
	"bar-foo-buzz-124567277757727277272727277272727272772727185162582118551009-loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong-test-backup",
}

func TestShortenName(t *testing.T) {
	require := require.New(t)
	r := rand.New(rand.NewSource(1))
	collision := map[string]struct{}{}
	for _, name := range names {
		l := r.Intn(56) + 8
		shortName, err := ShortenName(name, l)
		require.NoError(err)
		require.LessOrEqual(len(shortName), l, "short name too long")
		_, ok := collision[shortName]
		require.False(ok, "two names resulted in the same short name")
		collision[shortName] = struct{}{}
	}
}

func TestShortenName32(t *testing.T) {
	require := require.New(t)
	r := rand.New(rand.NewSource(1))
	collision := map[string]struct{}{}
	for _, name := range names {
		l := r.Intn(56) + 8
		shortName, err := ShortenName32(name, l)
		require.NoError(err)
		require.LessOrEqual(len(shortName), l, "short name too long")
		_, ok := collision[shortName]
		require.False(ok, "two names resulted in the same short name")
		collision[shortName] = struct{}{}
	}
}

func TestShortenName64(t *testing.T) {
	require := require.New(t)
	r := rand.New(rand.NewSource(1))
	collision := map[string]struct{}{}
	for _, name := range names {
		l := r.Intn(48) + 16
		shortName, err := ShortenName64(name, l)
		require.NoError(err)
		require.LessOrEqual(len(shortName), l, "short name too long")
		_, ok := collision[shortName]
		require.False(ok, "two names resulted in the same short name")
		collision[shortName] = struct{}{}
	}
}
