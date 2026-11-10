use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::ffi::{OsStrExt, OsStringExt};

const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const CANARY: &str = "m10_synthetic_canary";
const MAX_CASE_BYTES: usize = 64 * 1024;

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn environment() -> Environment {
    Environment(BTreeMap::from([
        (
            "PGCOPYDB_SOURCE_PGURI".into(),
            "dbname=synthetic_source".into(),
        ),
        (
            "PGCOPYDB_TARGET_PGURI".into(),
            "dbname=synthetic_target".into(),
        ),
        ("TMPDIR".into(), "/nonexistent/m10-synthetic-state".into()),
    ]))
}

fn argv(parts: &[&str]) -> Vec<OsString> {
    std::iter::once("rpgmigrate")
        .chain(parts.iter().copied())
        .map(OsString::from)
        .collect()
}

fn diagnostic(error: &Error, case: usize, category: &str) {
    assert!(
        matches!(error.kind, ErrorKind::BadArgs | ErrorKind::BadConfig),
        "seed={SEED:x} case={case} category={category} unexpected error class"
    );
    assert!(
        error.message.len() <= 1024 && !error.message.contains(CANARY),
        "seed={SEED:x} case={case} category={category} unsafe diagnostic"
    );
}

fn exercise(args: &[OsString], env: &Environment, case: &mut usize, category: &str) -> bool {
    assert!(args.iter().map(|arg| arg.len()).sum::<usize>() <= MAX_CASE_BYTES);
    let first_syntax = command()
        .try_get_matches_from(args)
        .map(|_| ())
        .map_err(|e| e.kind());
    let second_syntax = command()
        .try_get_matches_from(args)
        .map(|_| ())
        .map_err(|e| e.kind());
    assert_eq!(
        first_syntax, second_syntax,
        "seed={SEED:x} case={case} category={category}"
    );
    let first = contract_oracle(args, env);
    let second = contract_oracle(args, env);
    match (&first, &second) {
        (Ok(a), Ok(b)) => assert!(a == b, "seed={SEED:x} case={case} category={category}"),
        (Err(a), Err(b)) => {
            diagnostic(a, *case, category);
            assert!(
                a.kind == b.kind && a.message == b.message,
                "seed={SEED:x} case={case} category={category}"
            );
        }
        _ => panic!("seed={SEED:x} case={case} category={category} nondeterministic contract"),
    }
    // Parsing an Invocation is pure; executing it would cross the connection and mutation boundary.
    let first = parse(args, env);
    let second = parse(args, env);
    let accepted = match (first, second) {
        (Ok(a), Ok(b)) => {
            if category == "valid-seed" {
                assert!(
                    !matches!(a, Invocation::Unsupported(_)),
                    "seed={SEED:x} case={case} category={category} unsupported seed"
                );
            }
            assert!(
                std::mem::discriminant(&a) == std::mem::discriminant(&b),
                "seed={SEED:x} case={case} category={category}"
            );
            true
        }
        (Err(a), Err(b)) => {
            diagnostic(&a, *case, category);
            assert!(
                a.kind == b.kind && a.message == b.message,
                "seed={SEED:x} case={case} category={category}"
            );
            false
        }
        _ => panic!("seed={SEED:x} case={case} category={category} nondeterministic invocation"),
    };
    *case += 1;
    accepted
}

#[test]
fn bounded_mutation_cli_operator_argv() {
    let env = environment();
    // These routes mirror native operator lifecycle argv and the canonical CLI boundary fixtures.
    let seeds = [
        argv(&[
            "clone",
            "--follow",
            "--plugin",
            "pgoutput",
            "--table-jobs",
            "4",
            "--index-jobs",
            "4",
        ]),
        argv(&["follow", "--resume", "--durability", "durable"]),
        argv(&[
            "clone",
            "--use-copy-binary",
            "--split-tables-larger-than",
            "10kB",
            "--split-max-parts",
            "8",
        ]),
        argv(&["stream", "sentinel", "get", "--json"]),
        argv(&["stream", "sentinel", "set", "endpos", "0/10"]),
        argv(&["stream", "cleanup"]),
        argv(&["list", "progress", "--json"]),
        argv(&["compare", "schema"]),
        argv(&["compare", "data", "--json"]),
        argv(&[
            "ping",
            "--source",
            "service='synthetic source' password='m10_synthetic_canary'",
        ]),
        argv(&[
            "ping",
            "--target",
            "postgresql://synthetic:m10_synthetic_canary@invalid/synthetic?sslmode=require",
        ]),
        argv(&["version", "--json"]),
    ];
    let mut count = 0;
    let mut random = SEED;
    let mut hash = Sha256::new();
    for (key, value) in &env.0 {
        for bytes in [key.as_bytes(), value.as_bytes()] {
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
    }
    for args in &seeds {
        for arg in args {
            hash.update((arg.len() as u64).to_le_bytes());
            hash.update(arg.as_bytes());
        }
        assert!(
            exercise(args, &env, &mut count, "valid-seed"),
            "seed={SEED:x} case={count} category=valid-seed refused"
        );
        for index in 1..args.len() {
            let original = args[index].clone().into_vec();
            for end in 0..original.len() {
                let mut mutated = args.clone();
                mutated[index] = OsString::from_vec(original[..end].to_vec());
                exercise(&mutated, &env, &mut count, "truncation");
            }
            for _ in 0..8 {
                let offset = next(&mut random) as usize % original.len();
                for category in ["insert", "delete", "flip"] {
                    let mut bytes = original.clone();
                    match category {
                        "insert" => bytes.insert(offset, next(&mut random) as u8),
                        "delete" => {
                            bytes.remove(offset);
                        }
                        _ => bytes[offset] ^= 1 << (next(&mut random) % 8),
                    }
                    let mut mutated = args.clone();
                    mutated[index] = OsString::from_vec(bytes);
                    exercise(&mutated, &env, &mut count, category);
                }
            }
        }
    }
    for bytes in [
        vec![0],
        vec![0xff],
        vec![b'1', 0, b'2'],
        vec![b'x'; 32 * 1024],
    ] {
        let mut args = argv(&["follow", "--table-jobs"]);
        args.push(OsString::from_vec(bytes));
        assert!(!exercise(&args, &env, &mut count, "raw-argv-boundary"));
    }
    assert_eq!(count, 1537);
    eprintln!(
        "bounded_mutation_cli_operator_argv seed={SEED:x} seeds={} cases={count} canonical_corpus_sha256={}",
        seeds.len(),
        hash.finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}

#[test]
fn bounded_mutation_cli_settings_and_environment() {
    let mut count = 0;
    for (option, values) in [
        (
            "--table-jobs",
            vec!["0", "129", "2147483648", "1.0", "true", CANARY],
        ),
        (
            "--split-tables-larger-than",
            vec!["-1", "16EB", "1MB!", "true", CANARY],
        ),
        ("--durability", vec!["relaxed", "unknown", "true", CANARY]),
    ] {
        for value in values {
            assert!(!exercise(
                &argv(&["follow", option, value]),
                &environment(),
                &mut count,
                "invalid-setting"
            ));
        }
    }
    for (value, accepted) in [
        ("", false),
        ("0", false),
        ("-1", false),
        ("1.0", false),
        ("true", false),
        ("18446744073709551616", false),
        (CANARY, false),
        ("1", true),
        ("100", true),
        ("18446744073709551615", true),
    ] {
        let mut env = environment();
        env.0
            .insert("PGCOPYDB_APPLY_GROUP_TXNS".into(), value.into());
        assert_eq!(
            exercise(
                &argv(&["follow"]),
                &env,
                &mut count,
                "group-environment-boundary"
            ),
            accepted,
            "seed={SEED:x} case={count} category=group-environment-boundary"
        );
    }
    for key in [
        "PGCOPYDB_APPLY_GROUP_TXNS",
        "PGCOPYDB_TABLE_JOBS",
        "PGCOPYDB_USE_COPY_BINARY",
    ] {
        for value in [
            OsString::from(CANARY),
            OsString::from_vec(vec![0xff]),
            OsString::from_vec(vec![0]),
        ] {
            let mut env = environment();
            env.0.insert(key.into(), value);
            assert!(!exercise(
                &argv(&["follow"]),
                &env,
                &mut count,
                "invalid-environment"
            ));
        }
    }
    assert!(!exercise(
        &argv(&["follow", "--resume", "--restart"]),
        &environment(),
        &mut count,
        "conflicting-recovery"
    ));
    assert_eq!(count, 35);
    eprintln!("bounded_mutation_cli_settings_and_environment seed={SEED:x} cases={count}");
}
