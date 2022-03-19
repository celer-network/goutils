# new monitor util
Reduce queries and simplify config and API

Key insights:
- chain specific params:
  - BlkInterval(time.Duration) for updating block number
  - BlkDelay, MaxBlkDelta, ForwardBlkDelay are for setting correct from/to block in query filter
- for one contract, there should be query interval (time.Duration)

- monitor to ec, make sure from/to block is updated correctly. but update logic depends on logs' blknum
- test persist
- monitor to callback func, what affects which logs? only savedLogID and skip. but savedLogID may not be set to nil if nextFrom is the same from.
- cfg affects toBlk should be tested in CalcToBlkNum tests, same for CalcNextFromBlkNum

- enum test cases:
1. fresh start, db has no entry,  first FilterLogs from =