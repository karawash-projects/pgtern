# Limits and safety

- **`max_result_bytes`** is a connection attribute that caps the bytes one statement may return. It is `None` (no cap) by default. A result over the cap is read and discarded, the connection stays usable, and the statement raises `DataError`. Results are held in memory, so use a [server-side cursor](../guide/streaming.md) for large ones.
- **Reentrancy.** Using a connection from inside a `COPY` block, or from inside a notice or notify handler, raises `ProgrammingError` at once.
- **Hooks and handlers.** A query hook that fails is logged and ignored. A notice or notify handler that raises is logged too.
