use super::tests::{assert_poisoned, begin, feed, new_decoder, relation};
use super::*;

fn snapshot(decoder: &Decoder) -> Decoder {
    Decoder {
        limits: decoder.limits,
        active: decoder.active,
        last_end: decoder.last_end,
        relations: decoder.relations.clone(),
        types: decoder.types.clone(),
        origin: decoder.origin.clone(),
        poisoned: decoder.poisoned,
    }
}

fn exercise(decoder: &Decoder, bytes: &[u8]) -> bool {
    let mut candidate = snapshot(decoder);
    let active = candidate.active;
    let mut emissions = 0;
    let outcome = candidate.feed(bytes, &mut |message| {
        emissions += 1;
        assert!(message_bytes(&message).unwrap() <= decoder.limits.max_message_bytes);
        if let FollowMessage::Commit(commit) = message {
            let begin = active.unwrap();
            assert_eq!(commit.commit, begin.final_lsn);
            assert_eq!(commit.xid, begin.xid);
            assert_eq!(commit.commit_time_micros, begin.commit_time_micros);
            assert!(commit.end.0 >= commit.commit.0);
        }
        Ok(())
    });
    if outcome.is_err() {
        assert_eq!(emissions, 0);
        assert_poisoned(&mut candidate);
        false
    } else {
        assert!(emissions <= 1);
        assert!(candidate.metadata_bytes().unwrap() <= MAX_METADATA_BYTES);
        true
    }
}

// A dependency-free entry for arbitrary wire payloads, with ordinary tuple context.
pub(crate) fn fuzz_input(bytes: &[u8]) {
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    feed(&mut decoder, &relation(42, b'd', 1, &[25, 23])).unwrap();
    exercise(&decoder, bytes);
}

#[test]
fn retained_corpus_every_truncation_and_seeded_mutation_never_panics_or_escapes_poison() {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut checks = 0;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for major in [16, 18] {
        let fixture = super::tests::fixture(major);
        let mut decoder = super::tests::fixture_decoder(&fixture);
        for packet in fixture["packets"].as_array().unwrap() {
            let bytes = super::tests::unhex(packet["hex"].as_str().unwrap());
            for end in 0..bytes.len() {
                assert!(
                    !exercise(&decoder, &bytes[..end]),
                    "accepted truncated tag {} at {end}",
                    bytes[0]
                );
                checks += 1;
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(!exercise(&decoder, &trailing));
            checks += 1;
            for _ in 0..8 {
                let mut mutated = bytes.clone();
                let offset = (next() as usize) % mutated.len();
                mutated[offset] ^= 1 << (next() % 8);
                exercise(&decoder, &mutated);
                checks += 1;
            }
            feed(&mut decoder, &bytes).unwrap();
        }
    }
    for tag in 0..=255 {
        fuzz_input(&[tag]);
        checks += 1;
    }
    for length in 0..512 {
        let bytes = (0..length).map(|_| next() as u8).collect::<Vec<_>>();
        fuzz_input(&bytes);
        checks += 1;
    }
    assert!(checks > 500_000);
}
