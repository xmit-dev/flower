use super::*;
use serde_json::{json, Value};

fn metadata() -> Value {
    json!({
        "database": "a".repeat(32), "incarnation": "b".repeat(32),
        "currentEpoch": 3, "minEpoch": 1, "receiptBytes": 0, "receiptCount": 0,
        "maxReceiptBytes": null, "gcCursor": null, "gcComplete": true
    })
}

fn result<T>(value: anyhow::Result<T>) -> Result<T, String> {
    value.map_err(|error| format!("{error:#}"))
}

#[test]
fn borrowed_metadata_decode_preserves_owned_decode_values_and_errors() {
    let state = metadata();
    let session = json!({"id": "c".repeat(32), "incarnation": "b".repeat(32),
        "owner": "d".repeat(64), "epoch": 3, "acknowledgedThrough": 7, "closed": false});
    for (base, is_session) in [(state, false), (session, true)] {
        let mut cases = vec![
            base.clone(),
            Value::Null,
            json!([]),
            json!(false),
            json!("invalid"),
        ];
        let mut extra = base.clone();
        extra["unknown"] = json!({"nested": ["🌺", 1]});
        cases.push(extra);
        for key in base.as_object().unwrap().keys() {
            let mut missing = base.clone();
            missing.as_object_mut().unwrap().remove(key);
            cases.push(missing);
            for replacement in [
                Value::Null,
                json!([]),
                json!({}),
                json!(false),
                json!(1.5),
                json!("bad"),
            ] {
                let mut changed = base.clone();
                changed[key] = replacement;
                cases.push(changed);
            }
        }
        for value in cases {
            if is_session {
                let old = serde_json::from_value::<Session>(value.clone())
                    .context("invalid retry session metadata");
                let new = Session::deserialize(&value).context("invalid retry session metadata");
                assert_eq!(result(new), result(old), "{value}");
            } else {
                let old = serde_json::from_value::<State>(value.clone())
                    .context("invalid retention metadata")
                    .and_then(|state| {
                        state.validate()?;
                        Ok(Some(state))
                    });
                assert_eq!(result(decode(Some(&value))), result(old), "{value}");
            }
        }
    }
    assert_eq!(decode(None).unwrap(), None);
}

#[test]
fn borrowed_receipt_count_has_identical_serialization_and_exact_budgets() {
    for value in [
        Value::Null,
        json!("🌺\0\n\"\\"),
        json!({"z": [1.0, -0.0, 1e-7, 1e21, u64::MAX], "a": {"nested": true}}),
        json!({"wide": vec!["retained"; 4096]}),
    ] {
        for revision in [0, 1, u64::MAX] {
            let owned = Receipt {
                fingerprint: "quoted\"\n🌺".into(),
                revision,
                result: value.clone(),
                epoch: None,
            };
            let borrowed = BorrowedReceipt {
                fingerprint: &owned.fingerprint,
                revision,
                result: &value,
                epoch: None,
            };
            let bytes = serde_json::to_vec(&owned).unwrap();
            assert_eq!(serde_json::to_vec(&borrowed).unwrap(), bytes);
            for id in ["", "request:🌺"] {
                let expected = (id.len() + bytes.len()) as u64;
                assert_eq!(borrowed.encoded_bytes(id).unwrap(), expected);
                assert_eq!(receipt_bytes(id, &owned).unwrap(), expected);
            }
        }
    }
}

// Keep the previous admission order as a differential reference: metadata was
// decoded once by validate_request and once again before capacity accounting.
fn previous_capacity(
    snapshot: &Snapshot,
    id: &str,
    fingerprint: &str,
    value: &Value,
    credit: u64,
) -> anyhow::Result<()> {
    validate_request(snapshot, id)?;
    let Some(mut state) = status(snapshot)? else {
        return Ok(());
    };
    if let Some(receipt) = snapshot.requests.get(id) {
        ensure!(
            receipt.fingerprint == fingerprint,
            "REQUEST_ID_REUSED: request identity was already used for different content"
        );
        return Ok(());
    }
    let reserved = reserved_bytes(snapshot.data.get(RESERVED_BYTES))?
        .checked_sub(credit)
        .context("receipt credit exceeds the durable reservation")?;
    let receipt = Receipt {
        fingerprint: fingerprint.into(),
        revision: snapshot
            .revision
            .checked_add(1)
            .context("revision exhausted")?,
        result: value.clone(),
        epoch: None,
    };
    let bytes = id
        .len()
        .checked_add(super::super::encoded_json_len(&receipt)?)
        .context("receipt size exhausted")?;
    state.charge(
        u64::try_from(bytes).context("receipt size exceeds counter")?,
        1,
        reserved,
    )
}

#[test]
fn borrowed_capacity_validation_preserves_error_precedence_and_byte_boundaries() {
    let mut initial = Snapshot {
        revision: 7,
        ..Snapshot::default()
    };
    initial.data.insert(KEY.into(), metadata());
    let state = status(&initial).unwrap().unwrap();
    let id = scope_request_id(&state, "one intent");
    let value = json!({"value": [1.0, -0.0, "\n🌺"]});
    let fingerprint = "f".repeat(64);
    let receipt = Receipt {
        fingerprint: fingerprint.clone(),
        revision: 8,
        result: value.clone(),
        epoch: None,
    };
    let exact = receipt_bytes(&id, &receipt).unwrap();
    let mut cases = vec![Snapshot::default(), initial.clone()];
    for budget in [exact - 1, exact, exact + 10] {
        for reserved in [json!(0), json!(10), json!("bad")] {
            let mut snapshot = initial.clone();
            let mut policy = metadata();
            policy["maxReceiptBytes"] = json!(budget);
            snapshot.data.insert(KEY.into(), policy);
            snapshot.data.insert(RESERVED_BYTES.into(), reserved);
            cases.push(snapshot);
        }
    }
    for field in ["receiptCount", "receiptBytes"] {
        let mut snapshot = initial.clone();
        let mut policy = metadata();
        policy[field] = json!(u64::MAX);
        snapshot.data.insert(KEY.into(), policy);
        cases.push(snapshot);
    }
    let mut malformed = initial.clone();
    malformed
        .data
        .insert(KEY.into(), json!({"unexpected": true}));
    cases.push(malformed);
    let mut exhausted = initial.clone();
    exhausted.revision = u64::MAX;
    cases.push(exhausted);
    let mut duplicate = initial.clone();
    duplicate.requests.insert(id.clone(), receipt);
    duplicate.data.insert(
        RESERVED_BYTES.into(),
        json!("invalid but bypassed by retry"),
    );
    cases.push(duplicate);
    for snapshot in cases {
        for request in [id.as_str(), "unscoped", "f2:invalid"] {
            for fingerprint in [fingerprint.as_str(), "different"] {
                for credit in [0, 1, 10, u64::MAX] {
                    assert_eq!(
                        result(validate_capacity_for(
                            &snapshot,
                            request,
                            fingerprint,
                            &value,
                            credit
                        )),
                        result(previous_capacity(
                            &snapshot,
                            request,
                            fingerprint,
                            &value,
                            credit
                        )),
                        "request={request}, fingerprint={fingerprint}, credit={credit}",
                    );
                }
            }
        }
    }
    let mut budgeted = initial;
    let mut policy = metadata();
    policy["maxReceiptBytes"] = json!(exact);
    budgeted.data.insert(KEY.into(), policy.clone());
    assert!(validate_capacity_for(&budgeted, &id, &fingerprint, &value, 0).is_ok());
    policy["maxReceiptBytes"] = json!(exact - 1);
    budgeted.data.insert(KEY.into(), policy);
    assert!(
        validate_capacity_for(&budgeted, &id, &fingerprint, &value, 0)
            .unwrap_err()
            .to_string()
            .contains("RECEIPT_BUDGET_EXCEEDED")
    );
}
