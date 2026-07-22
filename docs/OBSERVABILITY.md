# Observability

`effect-broker` telemetry is correctness-focused. Metrics use a private
Prometheus registry and low-cardinality labels only. They never label by
`tenant_id`, `operation_key`, `effect_id`, raw arguments, outputs, or downstream
keys. Optional OpenTelemetry spans include only a short operation-key digest,
tool, contract version, status, and receipt id.

## Metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `effects_total` | counter | `result` | Submitted, deduplicated, conflicted, succeeded, failed, or unknown effects. |
| `outcome_unknown_effects` | gauge | `contract`, `safety` | Current observed `outcome_unknown` count. |
| `outcome_unknown_oldest_age_seconds` | gauge | `contract`, `safety` | Age of the oldest observed unknown effect. |
| `dispatch_attempts_total` | counter | `tool` | Target dispatch attempts. |
| `reconciliation_outcomes_total` | counter | `outcome` | Bounded reconciliation outcomes. |
| `lease_expirations_total` | counter | none | Expired leases converted by recovery code. |
| `fenced_stale_writes_total` | counter | none | CAS writes rejected from stale or zombie workers. |
| `payload_conflicts_total` | counter | none | Operation-key payload drift conflicts. |
| `op_latency_seconds` | histogram | `op` | `reserve`, `dispatch`, and `reconcile` latency. |
| `idempotency_retention_remaining_seconds` | gauge | none | Remaining target key retention when a retry is evaluated. |

`GET /metrics` returns the Prometheus text exposition format.

## Alert Rules

These alerts target broken correctness guarantees, not generic uptime.

```yaml
groups:
  - name: effect-broker-correctness
    rules:
      - alert: UnsafeOutcomeUnknown
        expr: outcome_unknown_effects{safety="unsafe"} > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: Unsafe effect has an unknown outcome

      - alert: OutcomeUnknownBeyondContractBound
        expr: outcome_unknown_oldest_age_seconds > 900
        for: 5m
        labels:
          severity: page
        annotations:
          summary: Unknown effect age exceeded the contract threshold

      - alert: RetryEvaluatedAfterIdempotencyRetentionExpiry
        expr: idempotency_retention_remaining_seconds < 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: Retry path evaluated after target idempotency retention expired

      - alert: PayloadDriftConflict
        expr: increase(payload_conflicts_total[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: Operation key was reused with a different payload

      - alert: FencedStaleWorkerWrite
        expr: increase(fenced_stale_writes_total[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: Stale worker write was fenced by compare-and-swap
```

Tune `OutcomeUnknownBeyondContractBound` per deployed contract. For example,
idempotent tools may use the target retention horizon; reconcilable tools should
use the declared settlement bound plus an operational margin.
