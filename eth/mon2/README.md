# new monitor util
Reduce queries and simplify config and API

Key insights:
- chain specific params:
  - BlkInterval(time.Duration) for updating block number
  - BlkDelay, MaxBlkDelta, ForwardBlkDelay are for setting correct from/to block in query filter
- for one contract, there should be query interval (time.Duration)