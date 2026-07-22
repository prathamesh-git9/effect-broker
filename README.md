# effect-broker

**A correctness boundary for side-effecting agent tools.** Agents, workflow
engines, and queues retry; charges, emails, and production changes must not
happen twice. `effect-broker` records a stable business intent before dispatch,
binds it to an immutable payload and a pinned tool contract, propagates one
downstream idempotency key across every attempt, and reconciles lost responses
against an authoritative target. When a target cannot support idempotency or
prove its state, the broker returns **`outcome_unknown`** and refuses an unsafe
retry — it never claims exactly-once for a target that cannot honor it.

See [docs/NEXT_PROJECT_SPEC.md](docs/NEXT_PROJECT_SPEC.md) for the full design and
guarantee table. This is early-stage; the proof-first build order (failure
contract and crash matrix before a polished API) is deliberate.

## License

MIT. See [LICENSE](LICENSE).
