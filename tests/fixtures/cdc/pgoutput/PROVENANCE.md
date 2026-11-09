# pgoutput v1 wire corpus

We recorded these packets from private PostgreSQL 16.15 and 18.6 containers using the SQL slot interface and synthetic rows.
Each corpus contains 558 complete pgoutput payloads, including ten committed transactions and a 512-row transaction that spilled 331,776 bytes on the source.
The slot interface returns the same plugin payloads that the replication transport wraps in XLogData.
The decoder tests consume the retained bytes directly.

We preserve full positional K tuples, NULL and empty text, unchanged TOAST, RI FULL rows, domain type metadata, origin metadata, identity columns, and multi-relation TRUNCATE with both option flags.
PostgreSQL omits the generated column from these default-publication wire tuples.
Native follow refuses all generated columns during source preflight; this wire example does not qualify generated-column follow.
The corpus records source spill statistics, server versions, source type bindings, packet LSNs and xids, and the exact capture SQL hash.
The unit tests assert the expected event inventory and tuple values independently of the decoder.

1. Verify the retained fixture inventory.

   ```sh
   python3 tests/fixtures/cdc/pgoutput/capture.py --verify
   ```

2. Run the inventory refusal controls.

   ```sh
   python3 tests/fixtures/cdc/pgoutput/capture.py --self-test
   ```

3. Reproduce the capture with Podman and the PostgreSQL 16 and 18 images.

   ```sh
   python3 tests/fixtures/cdc/pgoutput/capture.py
   ```

The capture creates and removes only its own randomly named containers and exposes no host ports.
Reproduction changes transaction timestamps, LSNs and xids, so it also regenerates the content manifest.

We checked the formats against the official [PG16 protocol documentation](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html) and [PG18 protocol documentation](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).
The [PG16 writer](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/logical/proto.c) and [PG18 writer](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/proto.c) establish full-width tuple output and the original type OID paired with its base type's qualified name in Y.
The [PG16 reorder buffer](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/logical/reorderbuffer.c) and [PG18 reorder buffer](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/reorderbuffer.c) establish the nonstreaming BEGIN final LSN and COMMIT LSN identity.
We take finite timestamp bounds from [PG16 timestamp.h](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/include/datatype/timestamp.h) and [PG18 timestamp.h](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/datatype/timestamp.h).
The normal transaction and object identifier thresholds come from [PG16 transam.h](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/include/access/transam.h) and [PG18 transam.h](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/transam.h).
The [PG16 heap header](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/include/access/htup_details.h) and [PG18 heap header](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/htup_details.h) cap table columns at 1600.

The required seeded mutation test checks every retained packet truncation, trailing bytes, random bit mutations, all byte-valued tags, and arbitrary payloads through the dependency-free `fuzz_input(&[u8])` test entry.
This is bounded mutation coverage.
Coverage-guided fuzzing remains a separate release gate.
