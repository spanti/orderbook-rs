# 11: Document the runtime contract and update the durable guide

**What to build:** Make the two write paths and the preliminary/durable acknowledgement contract impossible to misread. ADR 0003 and the glossary are already written; this issue updates the durable guide and the runtime module docs.

**Blocked by:** 05, 06, 07, 08, 09, 10.

**Status:** todo

- [ ] Update `docs/durable-storage.md` with a "Runtime path" section: apply-first, `Applied Acknowledgement` vs `Durable Acknowledgement`, durability watermark, snapshot gating, and the bounded journal queue.
- [ ] State explicitly that the synchronous `DurableMatchingEngine` keeps append-before-apply and the runtime does not.
- [ ] Add module-level docs to `src/runtime/`: thread ownership, channel topology, ordering guarantees, failure modes, and what is not covered (read API, market data, idempotency, sharding).
- [ ] Document the SLO measurement method and the fact that Windows is functional-only.
- [ ] Re-read `GLOSSARY.md` with the new runtime terms and remove any definition that now contradicts apply-first (Sequencer, Exact Recovery, Durable Matching Engine).
- [ ] Add a short "known deviations" section to the runtime docs matching the spec, so the project never claims HFT-grade latency without the missing pieces.
