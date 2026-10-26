# 06: Runtime event pipeline and snapshots

**What to build:** The matching runtime writes events instead of commands. The matching thread decides and applies every event, the journal appends `EventRecord`s, applied and durable receipts are defined per command event range, the durability watermark is an event sequence, and snapshots are captured only at command boundaries. The runtime works end to end for every command variant and recovers from the event log after a drop.

**Blocked by:** 04, 05

**Status:** ready-for-agent

- [ ] The matching thread decides, assigns a contiguous event sequence range and `command_id`, applies every event, and sends an applied receipt containing the events and the first/last event sequence.
- [ ] A durable receipt is sent only after the command's last event sequence is at or below the durability watermark.
- [ ] The journal writes `EventRecord`s and advances the watermark after `append_batch` returns.
- [ ] Pending durable entries are keyed by the command's last event sequence; no command sequence is used as the boundary.
- [ ] Snapshots are captured only at a command boundary and only when applied and durable event sequences are equal; cadence is in event sequences.
- [ ] Shutdown drains the ingress and waits for the durable and applied event sequences to meet within the timeout.
- [ ] An end-to-end runtime test covers submit, fill, cancel, modify, stop, and rejection, then drops and recovers from the event log.
- [ ] Existing runtime tests are migrated to event receipts; the runtime no longer uses `CommandOutcome`.
