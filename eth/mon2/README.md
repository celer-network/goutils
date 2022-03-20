# New onchain events monitor
- Reduce rpc calls by query logs on address without sepcifying topics
- Simpler APIs and config

## Usage
- only need to create a single monitor object for each chain
```go
mon, err := NewMonitor(ec, dal, perChainCfg)
```
- for each contract address, call MonAddr
```go
mon.MonAddr(perAddrCfg, cbfn)
```
- CAUTION: MonAddr is **blocking**, use `go mon.MonAddr(...)` for async

### Interfaces
- EthClient, support onchain rpc queries
```go
type EthClient interface {
	// so we don't need chainid in func arg
	ChainID(ctx context.Context) (*big.Int, error)
	// get latest block number, available since geth 1.9.22
	BlockNumber(ctx context.Context) (uint64, error)
	// get logs, q will not set topics to get all events from the address
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}
```
- DAL, to persist log block and index to resume after restart. key is `chainid-addr`. addr has no 0x prefix and are all lower case. eg. `5-16649fc66e802151ad4fb37a5925c5e588b413e9`. No longer support per addr + event as the block number and index are tracked at contract level, not event level.
```go
type DAL interface {
	GetMonitorBlock(key string) (uint64, int64, bool, error)
	SetMonitorBlock(key string, blockNum uint64, blockIdx int64) error
}
```
- EventCallback, handle event, first arg is event name if abi is provided in PerAddrCfg. all events from the same address will be delivered in sequence. It's the callback func's responsibility to switch on event name and handle differently.
```go
 func(string, types.Log)
```
### Configs
- PerChainCfg, chain specific params for interval to update head block number and block delay etc to set proper from and to block in query filter.
```go
type PerChainCfg struct {
	BlkIntv time.Duration // interval to call BlockNumber
	// below are params affecting from/to block in query filter
	BlkDelay, MaxBlkDelta, ForwardBlkDelay uint64
}
```
- PerAddrCfg, for each contract, mainly address, abi string and polling interval
```go
type PerAddrCfg struct {
	Addr    common.Address // contract addr
	ChkIntv time.Duration  // interval to call FilterLogs
	AbiStr  string         // XxxABI or XxxMetaData.ABI abi string from this contract's go binding, needed to match log topic to event name, if empty string, evname in callback is also empty
	FromBlk uint64         // optional. if > 0, means ignore persisted blocknum and use this for FromBlk in queries, don't set unless you know what you're doing
}
```
