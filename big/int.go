// drop in replacement of math/big.Int, json marshal/unmarshal has quotes, impl sql scan/value
// 3 ways to get *Int: new(big.Int), big.NewInt(int64), big.New(*math/big.Int)
// all *math/big.Int funcs are passthrough

package big

import (
	"fmt"
	mb "math/big"
)

type Int struct {
	mb.Int
}

func NewInt(x int64) *Int {
	return &Int{
		Int: *mb.NewInt(x),
	}
}

func New(i *mb.Int) *Int {
	return &Int{
		Int: *new(mb.Int).Set(i),
	}
}

// MarshalJSON always has quotes
func (i Int) MarshalJSON() ([]byte, error) {
	return []byte(`"` + i.String() + `"`), nil
}

// support both quotes and no quotes
func (i *Int) UnmarshalJSON(p []byte) error {
	plen := len(p)
	if string(p) == "null" || plen == 0 {
		// null or no value is treated as no-op so i is untouched
		return nil
	}
	if p[0] == '"' && p[plen-1] == '"' {
		p = p[1 : plen-1] // remove quotes
	}
	inner, ok := new(mb.Int).SetString(string(p), 10)
	if !ok {
		return fmt.Errorf("invalid bigint string: %s", p)
	}
	i.Int = *inner
	return nil
}

// ToInt returns a new *math/big.Int equal i.Int, to avoid unintentional change
// if read only, use i.Int directly
func (i Int) ToInt() *mb.Int {
	return new(mb.Int).Set(&i.Int)
}
