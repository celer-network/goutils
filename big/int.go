// drop in replacement of math/big.Int, json marshal/unmarshal has quotes
// 3 ways to get *Int: new(big.Int), big.NewInt(int64), big.New(*math/big.Int)
// all *math/big.Int funcs are passthrough

package big

import (
  mb "math/big"
)

type Int struct {
  mb.Int
}

func NewInt(x int64) *Int {
  
}

func New(i *mb.Int) *Int {
  
}
