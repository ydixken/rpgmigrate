CARGO ?= cargo
PYTHON ?= python3
PQ_LIB_DIR ?= $(CURDIR)/target/native/libpq/lib
export PQ_LIB_DIR
export STORAGE_ROOT
UNIT_FILTERS = --skip pg::live_tests:: --skip pg::prepared_live_tests:: --skip apply::checkpoint::live_tests:: --skip catalog::tests::live_ --skip copy::tests::live_

.PHONY: check fmt-check lint native native-check build test test-unit test-cli test-native test-harness test-storage-format test-storage-recovery storage-pilot test-db test-engine test-transport test-prepared test-authority test-db-self-check

check: native-check
	$(CARGO) check --locked --all-targets

fmt-check:
	$(CARGO) fmt --all -- --check

lint: fmt-check native-check
	$(CARGO) clippy --locked --all-targets -- -D warnings

native:
	$(PYTHON) tools/build-libpq.py

native-check:
	$(PYTHON) tools/build-libpq.py --verify

build: native-check
	$(CARGO) build --locked

test: test-unit test-cli test-native test-harness test-storage-format test-storage-recovery

test-unit: native-check
	$(CARGO) test --locked --lib -- --list $(UNIT_FILTERS) | awk '/: test$$/ { count++ } END { if (count == 0) { print "error: no Rust unit tests discovered"; exit 1 } }'
	$(CARGO) test --locked --lib --bins -- $(UNIT_FILTERS)

test-cli: build
	$(PYTHON) tests/cli_contract.py

test-native: native-check
	$(PYTHON) tests/test_native_libpq.py

test-harness:
	$(PYTHON) tests/support/test_harness.py

test-storage-format: native-check
	$(CARGO) test --locked --test storage_format -- --list | awk '/: test$$/ { count++ } END { if (count != 23) { print "error: expected 23 storage format and identity tests"; exit 1 } }'
	$(CARGO) test --locked --test storage_format -- --include-ignored

test-storage-recovery: native-check
	$(CARGO) test --locked --test storage_recovery -- --list | awk '/: test$$/ { count++ } END { if (count != 86) { print "error: expected 86 storage recovery and driver tests"; exit 1 } }'
	$(CARGO) test --locked --test storage_recovery -- --include-ignored --test-threads=1

storage-pilot: native-check
	$(CARGO) run --locked --release --example storage -- --root "$${STORAGE_ROOT:?set STORAGE_ROOT to a new directory}"

test-db: build test-engine
	$(PYTHON) tools/test-db.py -- $(CARGO) test --locked --test copy_integration -- --test-threads=1

test-engine: native-check
	$(PYTHON) tools/test-db.py --engine -- $(CARGO) test --locked --lib -- --ignored --test-threads=1

test-transport: native-check
	$(PYTHON) tools/test-db.py --transport -- $(CARGO) test --locked --lib pg::live_tests:: -- --ignored --test-threads=1

test-prepared: native-check
	$(PYTHON) tools/test-db.py --prepared -- $(CARGO) test --locked --lib pg::prepared_live_tests:: -- --ignored --test-threads=1

test-authority: native-check
	$(PYTHON) tools/test-db.py --authority -- $(CARGO) test --locked --lib apply::checkpoint::live_tests:: -- --ignored --test-threads=1

test-db-self-check:
	$(PYTHON) tools/test-db.py --self-check
