//! Differential tests (and a benchmark) for the direct JSON decoder against
//! the two-step `serde_json::Value` + `TryFrom` reference.

use std::{
    collections::BTreeMap,
    hint::black_box,
    time::Instant,
};

use proptest::{
    prelude::*,
    sample::{
        select,
        Index,
    },
};

use super::{
    json_deserialize,
    json_deserialize_bytes,
    json_deserialize_bytes_reference,
    json_deserialize_reference,
};
use crate::{
    ConvexArray,
    ConvexBytes,
    ConvexObject,
    ConvexValue,
    FieldName,
    JsonFloat,
    JsonInteger,
};

/// Both decoders must agree: equal values, or both errors.
fn assert_same_str(s: &str) -> Option<ConvexValue> {
    let new = json_deserialize(s);
    let old = json_deserialize_reference(s);
    let out = match (new, old) {
        (Ok(a), Ok(b)) => {
            assert_eq!(a, b, "decoders disagree on value for {s:?}");
            Some(a)
        },
        (Err(_), Err(_)) => None,
        (new, old) => panic!("decoders disagree for {s:?}: new={new:?} reference={old:?}"),
    };
    assert_same_bytes(s.as_bytes());
    out
}

fn assert_same_bytes(s: &[u8]) {
    match (
        json_deserialize_bytes(s),
        json_deserialize_bytes_reference(s),
    ) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "decoders disagree on value for {s:?}"),
        (Err(_), Err(_)) => {},
        (new, old) => panic!("byte decoders disagree for {s:?}: new={new:?} reference={old:?}"),
    }
}

fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

// ---------------------------------------------------------------------------
// Arbitrary ConvexValues
// ---------------------------------------------------------------------------

fn arb_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        4 => any::<f64>(),
        2 => any::<u64>().prop_map(f64::from_bits),
        1 => select(vec![
            0.0,
            -0.0,
            f64::NAN,
            -f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::MAX,
            f64::MIN,
            9007199254740993.0,
            1e300,
        ]),
        1 => any::<i64>().prop_map(|i| i as f64),
    ]
}

fn arb_field_name() -> impl Strategy<Value = FieldName> {
    prop_oneof![
        "[a-zA-Z_][a-zA-Z0-9_]{0,12}",
        // Printable ASCII (quotes and backslashes need escaping), not
        // starting with '$'.
        "[ -#%-~]?[ -~]{0,10}",
    ]
    .prop_filter_map("valid field name", |s| s.parse().ok())
}

fn arb_leaf() -> impl Strategy<Value = ConvexValue> {
    prop_oneof![
        Just(ConvexValue::Null),
        any::<bool>().prop_map(ConvexValue::from),
        any::<i64>().prop_map(ConvexValue::from),
        arb_f64().prop_map(ConvexValue::from),
        any::<String>().prop_map(|s| ConvexValue::try_from(s).unwrap()),
        "[ -~]{0,20}".prop_map(|s| ConvexValue::try_from(s).unwrap()),
        prop::collection::vec(any::<u8>(), 0..40)
            .prop_map(|b| ConvexValue::Bytes(ConvexBytes::try_from(b).unwrap())),
    ]
}

fn arb_convex_value() -> impl Strategy<Value = ConvexValue> {
    arb_leaf().prop_recursive(6, 96, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8)
                .prop_map(|v| ConvexValue::Array(ConvexArray::try_from(v).unwrap())),
            prop::collection::btree_map(arb_field_name(), inner, 0..8).prop_map(
                |m: BTreeMap<FieldName, ConvexValue>| {
                    ConvexValue::Object(ConvexObject::try_from(m).unwrap())
                }
            ),
        ]
    })
}

// ---------------------------------------------------------------------------
// Adversarial JSON text
// ---------------------------------------------------------------------------

const LITERALS: &[&str] = &[
    "null",
    "true",
    "false",
    "0",
    "-0",
    "-0.0",
    "0.0",
    "1",
    "-1",
    "1.5",
    "1e400",
    "-1e400",
    "1e308",
    "1.7976931348623157e308",
    "1.7976931348623159e308",
    "18446744073709551615",
    "18446744073709551616",
    "-9223372036854775808",
    "-9223372036854775809",
    "123456789012345678901234567890",
    "1e-400",
    "-1e-400",
    "4.9e-324",
    "2.4703282292062328e-324",
    "0e999999",
    "01",
    "1.",
    "-",
    "+1",
    ".5",
    "NaN",
    "Infinity",
    "\"\"",
    "\"a\"",
    "\"\\u0000\"",
    "\"\\ud800\"",
    "\"\\ud83d\\ude00\"",
    "\"é\"",
    "\"\\/\"",
    "[]",
    "{}",
    "[,]",
    "{,}",
    "[1,]",
    "{\"a\":1,}",
    "{\"a\"}",
    "{1:2}",
];

const SPECIAL_KEYS: &[&str] = &["\"$integer\"", "\"$float\"", "\"$bytes\""];

const KEYS: &[&str] = &[
    "\"a\"",
    "\"b\"",
    "\"\\u0061\"",
    "\"_id\"",
    "\"\"",
    "\"$integer\"",
    "\"$float\"",
    "\"$bytes\"",
    "\"\\u0024integer\"",
    "\"$x\"",
    "\"é\"",
    "\"\\u0001\"",
    "\"$serde_json::private::RawValue\"",
];

/// Candidate payloads for `$integer`/`$float`/`$bytes`, valid or not.
fn arb_b64_text() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..12).prop_map(|b| quote(&base64::encode(b))),
        any::<i64>().prop_map(|i| quote(&JsonInteger::encode(i))),
        arb_f64().prop_map(|f| quote(&JsonFloat::encode(f))),
        "[A-Za-z0-9+/=!_ -]{0,16}".prop_map(|s| quote(&s)),
        select(LITERALS).prop_map(String::from),
    ]
}

fn arb_key_text() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => select(KEYS).prop_map(String::from),
        2 => "[ -~]{0,5}".prop_map(|s| quote(&s)),
        1 => any::<String>().prop_map(|s| quote(&s)),
        1 => select(vec![1023usize, 1024, 1025]).prop_map(|n| quote(&"k".repeat(n))),
    ]
}

fn arb_leaf_text() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => select(LITERALS).prop_map(String::from),
        2 => any::<String>().prop_map(|s| quote(&s)),
        1 => arb_f64().prop_map(|f| format!("{f:?}")),
        1 => arb_f64().prop_map(|f| format!("{f:e}")),
        1 => any::<i64>().prop_map(|i| i.to_string()),
        1 => any::<u64>().prop_map(|i| i.to_string()),
        3 => (select(SPECIAL_KEYS), arb_b64_text()).prop_map(|(k, v)| format!("{{{k}:{v}}}")),
        1 => arb_convex_value().prop_map(|v| v.json_serialize().unwrap()),
    ]
}

fn obj(entries: &[(String, String)]) -> String {
    let body: Vec<String> = entries.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!("{{{}}}", body.join(","))
}

fn arb_json_text() -> impl Strategy<Value = String> {
    arb_leaf_text().prop_recursive(5, 64, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(|xs| format!("[{}]", xs.join(","))),
            prop::collection::vec((arb_key_text(), inner.clone()), 0..6).prop_map(|kvs| obj(&kvs)),
            // Embedded raw value (serde_json `raw_value` feature).
            inner
                .clone()
                .prop_map(|t| format!("{{\"$serde_json::private::RawValue\":{}}}", quote(&t))),
            (inner.clone(), arb_key_text(), inner.clone()).prop_map(|(t, k, v)| format!(
                "{{\"$serde_json::private::RawValue\":{},{k}:{v}}}",
                quote(&t)
            )),
            // Special key whose value is a raw value wrapping a payload.
            (select(SPECIAL_KEYS), arb_b64_text(), any::<bool>()).prop_map(|(k, v, pad)| {
                let v = if pad { format!(" {v} ") } else { v };
                format!(
                    "{{{k}:{{\"$serde_json::private::RawValue\":{}}}}}",
                    quote(&v)
                )
            }),
            // Special key with an arbitrary value.
            (select(SPECIAL_KEYS), inner.clone()).prop_map(|(k, v)| format!("{{{k}:{v}}}")),
            // Duplicate special keys: a later value can rescue an earlier one.
            (
                select(SPECIAL_KEYS),
                inner.clone(),
                arb_b64_text(),
                any::<bool>()
            )
                .prop_map(|(k, v, b, swap)| {
                    let (x, y) = if swap { (b, v) } else { (v, b) };
                    obj(&[(k.to_owned(), x), (k.to_owned(), y)])
                }),
            // Duplicate regular keys, possibly with other keys between.
            (
                arb_key_text(),
                inner.clone(),
                inner.clone(),
                prop::option::of((arb_key_text(), inner.clone()))
            )
                .prop_map(|(k, v1, v2, mid)| {
                    let mut e = vec![(k.clone(), v1)];
                    e.extend(mid);
                    e.push((k, v2));
                    obj(&e)
                }),
            // Whitespace.
            inner.clone().prop_map(|t| format!(" \n\t{t}\r ")),
        ]
    })
}

#[derive(Debug, Clone)]
enum Mutation {
    None,
    Truncate(Index),
    Delete(Index),
    Insert(Index, char),
    Replace(Index, char),
    Append(String),
}

fn arb_mutation() -> impl Strategy<Value = Mutation> {
    let ch = select(vec![
        '"', '\\', '{', '}', '[', ']', ',', ':', '$', '-', '0', '1', 'e', '.', ' ', 'a', '\u{0}',
        '\u{1}', 'é', 'n',
    ]);
    prop_oneof![
        4 => Just(Mutation::None),
        2 => any::<Index>().prop_map(Mutation::Truncate),
        1 => any::<Index>().prop_map(Mutation::Delete),
        1 => (any::<Index>(), ch.clone()).prop_map(|(i, c)| Mutation::Insert(i, c)),
        1 => (any::<Index>(), ch).prop_map(|(i, c)| Mutation::Replace(i, c)),
        1 => select(vec![" ", "x", ",", "}", "]", " null", "{}"]).prop_map(|s| Mutation::Append(s.to_owned())),
    ]
}

fn char_boundary(s: &str, idx: Index) -> usize {
    let bounds: Vec<usize> = s
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(s.len()))
        .collect();
    bounds[idx.index(bounds.len())]
}

fn apply(mut s: String, m: Mutation) -> String {
    match m {
        Mutation::None => {},
        Mutation::Truncate(i) => {
            let at = char_boundary(&s, i);
            s.truncate(at);
        },
        Mutation::Delete(i) => {
            let at = char_boundary(&s, i);
            if at < s.len() {
                s.remove(at);
            }
        },
        Mutation::Insert(i, c) => {
            let at = char_boundary(&s, i);
            s.insert(at, c);
        },
        Mutation::Replace(i, c) => {
            let at = char_boundary(&s, i);
            if at < s.len() {
                s.remove(at);
            }
            s.insert(at, c);
        },
        Mutation::Append(t) => s.push_str(&t),
    }
    s
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    /// Every value round-trips through `json_serialize` (the persistence
    /// encoding) identically with both decoders.
    #[test]
    fn roundtrip_matches_reference(v in arb_convex_value()) {
        let s = v.json_serialize().unwrap();
        let new = json_deserialize(&s).unwrap();
        let old = json_deserialize_reference(&s).unwrap();
        prop_assert_eq!(&new, &v);
        prop_assert_eq!(&old, &v);
        prop_assert_eq!(&json_deserialize_bytes(s.as_bytes()).unwrap(), &v);
        prop_assert_eq!(&json_deserialize_bytes_reference(s.as_bytes()).unwrap(), &v);
    }

    /// Serialized values under random mutations.
    #[test]
    fn mutated_serialized_values_match_reference(v in arb_convex_value(), m in arb_mutation()) {
        let s = apply(v.json_serialize().unwrap(), m);
        assert_same_str(&s);
    }

    /// Adversarial JSON grammar (special objects of every shape, duplicate
    /// keys, raw values, invalid field names, extreme numbers) with random
    /// mutations.
    #[test]
    fn adversarial_json_matches_reference(t in arb_json_text(), m in arb_mutation()) {
        assert_same_str(&apply(t, m));
    }

    /// Arbitrary strings and raw bytes (invalid UTF-8 for the slice decoder).
    #[test]
    fn random_input_matches_reference(
        s in any::<String>(),
        b in prop::collection::vec(any::<u8>(), 0..64),
        doc in arb_convex_value(),
        i in any::<Index>(),
        byte in any::<u8>(),
    ) {
        assert_same_str(&s);
        assert_same_bytes(&b);
        let mut bytes = doc.json_serialize().unwrap().into_bytes();
        if !bytes.is_empty() {
            let at = i.index(bytes.len());
            bytes[at] = byte;
        }
        assert_same_bytes(&bytes);
    }
}

/// Guard against a generator that only produces errors (or only successes):
/// both outcomes must be well represented in the adversarial corpus.
#[test]
fn adversarial_generator_covers_both_outcomes() {
    use proptest::{
        strategy::ValueTree,
        test_runner::TestRunner,
    };
    let mut runner = TestRunner::deterministic();
    let strategy = (arb_json_text(), arb_mutation());
    let (mut ok, mut err) = (0usize, 0usize);
    for _ in 0..2000 {
        let (t, m) = strategy.new_tree(&mut runner).unwrap().current();
        match assert_same_str(&apply(t, m)) {
            Some(_) => ok += 1,
            None => err += 1,
        }
    }
    println!("adversarial corpus: {ok} decoded, {err} rejected");
    assert!(ok >= 200 && err >= 200, "ok={ok} err={err}");
}

#[test]
fn nesting_limits_match_reference() {
    for depth in 0..=200usize {
        let arrays = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        let objects = format!("{}null{}", "{\"a\":".repeat(depth), "}".repeat(depth));
        let specials = format!(
            "{}{{\"$integer\":\"AAAAAAAAAAA=\"}}{}",
            "[".repeat(depth),
            "]".repeat(depth)
        );
        for s in [&arrays, &objects, &specials] {
            assert_same_str(s);
        }
    }
    // Raw values restart serde_json's recursion limit.
    let mut t = "[".repeat(100) + &"]".repeat(100);
    for _ in 0..4 {
        t = format!(
            "{}{{\"$serde_json::private::RawValue\":{}}}{}",
            "[".repeat(60),
            quote(&t),
            "]".repeat(60)
        );
        assert_same_str(&t);
    }
}

#[test]
fn hand_picked_cases_match_reference() {
    let int = JsonInteger::encode(-7);
    let nan = JsonFloat::encode(f64::NAN);
    let neg_zero = JsonFloat::encode(-0.0);
    let one = JsonFloat::encode(1.0);
    let cases: Vec<String> = vec![
        format!("{{\"$integer\":\"{int}\"}}"),
        format!("{{\"$integer\":\"{int}\",\"$integer\":\"{int}\"}}"),
        format!("{{\"$integer\":5,\"$integer\":\"{int}\"}}"),
        format!("{{\"$integer\":\"{int}\",\"$integer\":5}}"),
        format!("{{\"$integer\":\"bad\",\"$integer\":\"{int}\"}}"),
        format!("{{\"\\u0024integer\":\"{int}\"}}"),
        format!("{{\"$integer\":\"{int}\",\"$float\":\"{nan}\"}}"),
        format!("{{\"$integer\":\"{int}\",\"a\":1}}"),
        format!("{{\"a\":1,\"$integer\":\"{int}\"}}"),
        "{\"$integer\":\"AAAAAAAAAA==\"}".to_owned(),
        "{\"$integer\":\"AAAAAAAAAAAAAA==\"}".to_owned(),
        "{\"$integer\":\"!!!!\"}".to_owned(),
        format!("{{\"$float\":\"{nan}\"}}"),
        format!("{{\"$float\":\"{neg_zero}\"}}"),
        format!("{{\"$float\":\"{one}\"}}"),
        "{\"$bytes\":\"\"}".to_owned(),
        "{\"$bytes\":\"AQID\"}".to_owned(),
        "{\"$bytes\":\"AQI\"}".to_owned(),
        "{\"$bytes\":null}".to_owned(),
        "{\"$bytes\":{\"$serde_json::private::RawValue\":\"\\\"AQID\\\"\"}}".to_owned(),
        "{\"$bytes\":{\"$serde_json::private::RawValue\":\" \\\"AQID\\\" \"}}".to_owned(),
        "{\"$bytes\":{\"$serde_json::private::RawValue\":\"[\"}}".to_owned(),
        "{\"$bytes\":{\"$serde_json::private::RawValue\":\"5\"}}".to_owned(),
        "{\"$serde_json::private::RawValue\":\"[1,2]\"}".to_owned(),
        "{\"$serde_json::private::RawValue\":\"[1,2]\",\"a\":1}".to_owned(),
        "{\"$serde_json::private::RawValue\":5}".to_owned(),
        "{\"$serde_json::private::RawValue\":\"{\\\"$integer\\\":\\\"AAAAAAAAAAA=\\\"}\"}"
            .to_owned(),
        "{\"a\":1,\"$serde_json::private::RawValue\":\"1\"}".to_owned(),
        "{\"a\":{\"$integer\":\"bad\"},\"a\":1}".to_owned(),
        "{\"a\":1,\"a\":{\"$integer\":\"bad\"}}".to_owned(),
        "{\"a\":{\"$integer\":\"bad\"},\"b\":2,\"a\":1}".to_owned(),
        "{\"a\":1,\"\\u0061\":2}".to_owned(),
        "{\"$x\":1,\"$x\":2}".to_owned(),
        "{\"$x\":1}".to_owned(),
        "{\"\":1}".to_owned(),
        "{\"é\":1}".to_owned(),
        format!("{{\"{}\":1}}", "k".repeat(1024)),
        format!("{{\"{}\":1}}", "k".repeat(1025)),
        "-0".to_owned(),
        "-0.0".to_owned(),
        "[-0,0,-0.0,1e400]".to_owned(),
        "1e-400".to_owned(),
        "18446744073709551616".to_owned(),
        "[1,2,3] x".to_owned(),
        "  [1]  ".to_owned(),
        "".to_owned(),
        "\"\\ud800\"".to_owned(),
    ];
    for s in cases {
        assert_same_str(&s);
    }
}

#[test]
fn size_limits_match_reference() {
    // Around the 32 MiB system size limit, and field/array count limits.
    for len in [(1 << 25) - 3, (1 << 25) - 2, (1 << 25) - 1] {
        assert_same_str(&quote(&"x".repeat(len)));
    }
    for n in [1023usize, 1024, 1025, 8191, 8192, 8193] {
        let arr = format!("[{}]", vec!["0"; n].join(","));
        assert_same_str(&arr);
        let fields: Vec<String> = (0..n).map(|i| format!("\"f{i}\":0")).collect();
        assert_same_str(&format!("{{{}}}", fields.join(",")));
    }
}

// ---------------------------------------------------------------------------
// Benchmark: cargo test -p value --release -- --ignored --nocapture bench_json
// ---------------------------------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn word(&mut self, min: u64, max: u64) -> String {
        const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJ0123456789";
        let len = min + self.below(max - min + 1);
        (0..len)
            .map(|_| ALPHA[self.below(ALPHA.len() as u64) as usize] as char)
            .collect()
    }
}

fn bench_document(rng: &mut Rng) -> ConvexValue {
    fn s(v: String) -> ConvexValue {
        ConvexValue::try_from(v).unwrap()
    }
    fn o(fields: Vec<(&str, ConvexValue)>) -> ConvexValue {
        let m: BTreeMap<FieldName, ConvexValue> = fields
            .into_iter()
            .map(|(k, v)| (k.parse().unwrap(), v))
            .collect();
        ConvexValue::Object(ConvexObject::try_from(m).unwrap())
    }
    fn a(items: Vec<ConvexValue>) -> ConvexValue {
        ConvexValue::Array(ConvexArray::try_from(items).unwrap())
    }
    let tags = (0..rng.below(6)).map(|_| s(rng.word(3, 10))).collect();
    let items = (0..rng.below(4))
        .map(|_| {
            o(vec![
                ("sku", s(rng.word(8, 12))),
                ("qty", ConvexValue::from(rng.below(20) as f64)),
                (
                    "price",
                    ConvexValue::from(rng.below(100_000) as f64 / 100.0),
                ),
            ])
        })
        .collect();
    let mut fields = vec![
        ("_id", s(rng.word(32, 32))),
        (
            "_creationTime",
            ConvexValue::from(1.7e12 + rng.below(1 << 30) as f64 + 0.25),
        ),
        ("name", s(rng.word(8, 30))),
        ("email", s(format!("{}@example.com", rng.word(5, 12)))),
        ("age", ConvexValue::from(rng.below(90) as f64)),
        (
            "score",
            ConvexValue::from(rng.next() as f64 / u64::MAX as f64),
        ),
        ("active", ConvexValue::from(rng.below(2) == 0)),
        ("tags", a(tags)),
        (
            "profile",
            o(vec![
                ("bio", s(rng.word(20, 160))),
                ("city", s(rng.word(4, 14))),
                (
                    "lat",
                    ConvexValue::from(rng.below(180_000) as f64 / 1000.0 - 90.0),
                ),
                (
                    "lng",
                    ConvexValue::from(rng.below(360_000) as f64 / 1000.0 - 180.0),
                ),
                (
                    "prefs",
                    o(vec![
                        ("theme", s(rng.word(4, 6))),
                        ("notifications", ConvexValue::from(rng.below(2) == 0)),
                        ("limit", ConvexValue::from(rng.below(1000) as i64)),
                    ]),
                ),
            ]),
        ),
        ("counter", ConvexValue::from(rng.next() as i64)),
        ("items", a(items)),
        ("deletedAt", ConvexValue::Null),
    ];
    if rng.below(3) == 0 {
        let b: Vec<u8> = (0..8 + rng.below(24)).map(|_| rng.next() as u8).collect();
        fields.push((
            "blob",
            ConvexValue::Bytes(ConvexBytes::try_from(b).unwrap()),
        ));
    }
    o(fields)
}

fn time_ns_per_doc(
    docs: &[String],
    reps: usize,
    f: fn(&str) -> anyhow::Result<ConvexValue>,
) -> f64 {
    let start = Instant::now();
    for _ in 0..reps {
        for d in docs {
            black_box(f(black_box(d)).unwrap());
        }
    }
    start.elapsed().as_nanos() as f64 / (reps * docs.len()) as f64
}

#[test]
#[ignore]
fn bench_json_decode() {
    let mut rng = Rng(0xC0FFEE);
    let docs: Vec<String> = (0..2000)
        .map(|_| bench_document(&mut rng).json_serialize().unwrap())
        .collect();
    let sizes: Vec<usize> = docs.iter().map(|d| d.len()).collect();
    let avg = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
    let (min, max) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
    for d in &docs {
        assert_eq!(
            json_deserialize(d).unwrap(),
            json_deserialize_reference(d).unwrap()
        );
    }
    let reps = 50;
    // `JSON_BENCH_ONLY=reference|direct` runs a single decoder (for `perf stat`).
    if let Ok(only) = std::env::var("JSON_BENCH_ONLY") {
        let f = match only.as_str() {
            "reference" => json_deserialize_reference,
            "direct" => json_deserialize,
            other => panic!("unknown JSON_BENCH_ONLY={other}"),
        };
        let ns = time_ns_per_doc(&docs, reps * 7, f);
        println!(
            "json decode bench ({only} only): {ns:.0} ns/doc over {} decodes",
            reps * 7 * docs.len()
        );
        return;
    }
    // Warm up both, then alternate rounds and keep the best of each.
    time_ns_per_doc(&docs, 5, json_deserialize_reference);
    time_ns_per_doc(&docs, 5, json_deserialize);
    let (mut old_best, mut new_best) = (f64::MAX, f64::MAX);
    let (mut old_all, mut new_all) = (vec![], vec![]);
    for _ in 0..7 {
        let o = time_ns_per_doc(&docs, reps, json_deserialize_reference);
        let n = time_ns_per_doc(&docs, reps, json_deserialize);
        old_best = old_best.min(o);
        new_best = new_best.min(n);
        old_all.push(o);
        new_all.push(n);
    }
    old_all.sort_by(f64::total_cmp);
    new_all.sort_by(f64::total_cmp);
    let (old_med, new_med) = (old_all[3], new_all[3]);
    println!(
        "json decode bench: {} docs, {avg:.0} B avg (min {min}, max {max}), {reps} reps x 7 rounds",
        docs.len()
    );
    println!(
        "  reference (serde_json::Value + TryFrom): best {old_best:.0} ns/doc, median \
         {old_med:.0} ns/doc ({:.0} MB/s)",
        avg * 1000.0 / old_med
    );
    println!(
        "  direct    (DeserializeSeed):             best {new_best:.0} ns/doc, median \
         {new_med:.0} ns/doc ({:.0} MB/s)",
        avg * 1000.0 / new_med
    );
    println!(
        "  speedup: {:.2}x (median), {:.2}x (best)",
        old_med / new_med,
        old_best / new_best
    );
}
