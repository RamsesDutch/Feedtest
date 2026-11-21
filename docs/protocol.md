# Adapter protocol notes

References checked 21 November 2026

- [Kraken Spot WebSocket v2 book](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)
- [Kraken checksum rules](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)

The adapter supports one configured symbol and subscription depth 10. It parses
JSON numeric fractions with Decimal, applies all updates in listed order,
removes zero quantities, truncates to subscribed depth, then checks unsigned
CRC32. The string is asks ascending then bids descending, price followed by
quantity for each level, with decimal points and leading zeros removed.
Formatting preserves numeric precision and trailing decimal zeros from the wire.

The test suite includes Kraken's published checksum example with expected
CRC32 3310070434, plus locally authored fixtures. The synthetic demo fixture
has one level per side and a valid checksum on each message. It is sufficient
for transport and missing-state demonstrations; it does not represent real
market conditions or a complete 10-level recording.

The independent example implements its own checksum code. Both implementations
follow the same published protocol, so independent external SDK use remains
an additional interoperability milestone.

Capture baselines must contain a valid snapshot before any update. Invalid
baselines are rejected before a server starts, preserving the distinction
between a damaged recording and an intentionally injected failure.

Session names are a local testing extension for disconnect-once behavior.
Each new connection receives a fresh replay from snapshot. The session name
does not request a historical cursor or authenticate a user. Session state is
in memory and resets when the server restarts.
