CARGO ?= cargo
PYTHON ?= python3
PQ_LIB_DIR ?= $(CURDIR)/target/native/libpq/lib
export PQ_LIB_DIR
UNIT_FILTERS = --skip pg::live_tests:: --skip catalog::tests::live_ --skip copy::tests::live_

.PHONY: check fmt-check lint native native-check build test test-unit test-cli test-native test-harness test-storage-format test-db test-engine test-transport test-db-self-check

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

test: test-unit test-cli test-native test-harness test-storage-format

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
	$(CARGO) test --locked --test storage_format -- --list | awk '/: test$$/ { count++ } END { if (count != 22) { print "error: expected 22 storage format tests"; exit 1 } }'
	$(CARGO) test --locked --test storage_format -- --include-ignored

test-db: build test-engine
	$(PYTHON) tools/test-db.py -- $(CARGO) test --locked --test copy_integration -- --test-threads=1

test-engine: native-check
	$(PYTHON) tools/test-db.py --engine -- $(CARGO) test --locked --lib -- --ignored --test-threads=1

test-transport: native-check
	$(PYTHON) tools/test-db.py --transport -- $(CARGO) test --locked --lib pg::live_tests:: -- --ignored --test-threads=1

test-db-self-check:
	$(PYTHON) tools/test-db.py --self-check
