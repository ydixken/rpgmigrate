# test_decoding decoder fixtures

We retain six messages from PostgreSQL 18.6 regression output in `postgresql-18.6.json`.
Each entry records its source file and line.
The archive SHA-256 identifies the reviewed source archive used for extraction.
These are psql-rendered message payloads with display padding removed, not recorded replication envelopes.
PostgreSQL attribution and license remain in the repository's third-party notices.
The unit tests construct separate XLogData envelopes and never infer envelope positions from this output.

The first decoder profile requires `include-xids=true`, `include-timestamp=false`, `skip-empty-xacts=false`, `only-local=false`, `include-rewrites=false`, and `stream-changes=false` where the server supports those options.
Disable slot two-phase decoding.
The decoder refuses logical messages, streaming output, prepared transactions, legacy system OIDs, unsupported types, and identities other than DEFAULT with a verified primary key.
PostgreSQL 9.6 through 10 cannot emit test_decoding TRUNCATE changes.
The receiver must enforce that qualification limitation before claiming complete change capture on those servers.

We retain one last-emitted relation key per transaction.
Emit borrowed relation metadata before a row with a different key.
Reset the key at BEGIN, transaction completion, and error.
Emit required TRUNCATE contexts in list order before its event.
Frame order does not prove persistence, synchronization, or source acknowledgment.
