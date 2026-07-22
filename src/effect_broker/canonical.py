"""Canonical serialization and operation fingerprinting.

Deduplication is only as sound as the fingerprint behind it. If two processes
could canonicalize the same request to different bytes — because one sorted keys
and the other didn't, or one allowed ``NaN`` — the broker could reserve the same
operation twice or, worse, treat two different operations as one. So canonical
JSON here is strict and total: it rejects anything it cannot reproduce
identically everywhere (non-string keys, NaN/infinity, unknown types), rather
than silently coercing it the way ``json.dumps`` does (``{1: "a"}`` would
otherwise become ``{"1": "a"}``).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from effect_broker.errors import CanonicalizationError


def _validate(value: Any, path: str) -> None:
    """Reject any value that cannot be canonicalized reproducibly.

    Booleans are checked before ints because ``bool`` is a subclass of ``int``
    and both are valid; the ordering only matters for clarity of error messages.
    """
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError(f"non-finite float at {path}: {value!r}")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError(
                    f"non-string object key at {path}: {key!r}"
                )
            _validate(item, f"{path}.{key}")
        return
    # A str is a Sequence, but it is handled above; guard against bytes too.
    if isinstance(value, (bytes, bytearray)):
        raise CanonicalizationError(f"bytes are not serializable at {path}")
    if isinstance(value, Sequence):
        for index, item in enumerate(value):
            _validate(item, f"{path}[{index}]")
        return
    raise CanonicalizationError(
        f"unsupported type at {path}: {type(value).__name__}"
    )


def canonical_json(value: Any) -> str:
    """Return the one canonical JSON string for ``value``.

    Sorted keys, compact separators, no ASCII escaping, and ``allow_nan=False``
    give a single reproducible encoding. Validation runs first so a bad value
    fails loudly instead of producing an encoding another process would reject.
    """
    _validate(value, "$")
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def request_hash(
    tool: str,
    arguments: Mapping[str, Any],
    contract_name: str,
    contract_version: str,
) -> str:
    """The SHA-256 that binds an operation key to an exact payload + contract.

    Reusing an operation key with a *different* hash is a conflict the broker
    refuses to dispatch — it means the same business intent was submitted with
    changed arguments or under a different contract version, which must never be
    silently collapsed onto the in-flight effect.
    """
    canonical = canonical_json(
        {
            "tool": tool,
            "arguments": arguments,
            "contract_name": contract_name,
            "contract_version": contract_version,
        }
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def downstream_key(tenant_key: bytes, tenant_id: str, operation_key: str) -> str:
    """Derive the stable key propagated to the target on every attempt.

    HMAC (not a bare hash) so the value cannot be guessed or forged by a caller
    who knows the operation key but not the tenant secret. The same
    ``(tenant_id, operation_key)`` always yields the same downstream key, which is
    what lets a redispatch after a crash reuse the target's idempotency slot
    instead of creating a second effect. The NUL separator prevents
    ``(a, bc)`` and ``(ab, c)`` from colliding.
    """
    message = f"{tenant_id}\0{operation_key}".encode()
    digest = hmac.new(tenant_key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
