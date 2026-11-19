"""Shared wire detail between sender and proxy: the optional run-id header.

A client (normally the sender) may prefix its TCP stream with one ASCII line::

    TCPFAULT-RUNID <run-id>\n

The proxy consumes this line, tags the connection with ``<run-id>`` and does
NOT forward it to the receiver.  The sample payload therefore keeps its exact
byte count, SHA-256, pause point and FIN order; the header exists only on the
sender<->proxy hop (the Docker Compose internal link).

Clients that do not send a well-formed header (e.g. the raw-socket smoke
tests, or `nc` straight into the proxy's TCP ingress) have every byte
forwarded verbatim: the proxy only claims the line when it matches MAGIC +
a valid run-id + newline exactly, otherwise every probed byte is treated as
payload.
"""

from __future__ import annotations

import re

MAGIC = b"TCPFAULT-RUNID "
MAX_RUN_ID_LEN = 64
RUN_ID_PATTERN = r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}"
_RUN_ID_RE = re.compile(RUN_ID_PATTERN)


def is_valid_run_id(run_id: str) -> bool:
    return _RUN_ID_RE.fullmatch(run_id) is not None


def encode_header(run_id: str) -> bytes:
    """Serialize the run-id header line (raises on an invalid run-id)."""
    if not is_valid_run_id(run_id):
        raise ValueError(f"invalid run id for header: {run_id!r}")
    return MAGIC + run_id.encode("ascii") + b"\n"
