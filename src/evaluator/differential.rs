//! Compare the Rust coordinator against the original JavaScript state machine.
use super::*;
use std::collections::BTreeSet;

const APPLICATION: &str = r#"
let calls = 0;
const records = {kind:'collection',name:'records'};
const config = {kind:'collection',name:'config'};
const ref = name => ({kind:'derived',name});
const definitions = {
  row: {kind:'derived',compute:(ctx,key) => {
    const value=ctx.get(records,key); ++calls;
    if(value && value.fail) throw new Error('row unavailable');
    return {value,local:calls};
  }},
  total: {kind:'derived',compute:(ctx,group) => {
    const rows=ctx.query({kind:'query',collection:'records',fields:['group'],value:group});
    let total=0; for(const row of rows) total += ctx.get(ref('row'),row.key).value.amount;
    return total;
  }},
  switched: {kind:'derived',compute:ctx => ctx.get(ref('row'), ctx.get(config,'selected') || 'a')},
  clock: {kind:'derived',compute:ctx => ctx.now()},
  read: {kind:'queryMethod',compute:(ctx,args) => ctx.get(ref(args.name), args.args)},
  scan: {kind:'queryMethod',compute:ctx => ctx.scan(records)},
  stage: {kind:'mutationMethod',compute:(ctx,args) => {
    const target=ref('total');
    ctx.set(records,args.key,{key:args.key,group:args.group,amount:args.amount});
    const first=ctx.get(target,args.group);
    ctx.set(records,args.key,{key:args.key,group:args.group,amount:args.amount+1});
    if(args.keep) ctx.materialize(target,args.group); else ctx.unmaterialize(target,args.group);
    return {first,last:ctx.get(target,args.group)};
  }},
  reject: {kind:'mutationMethod',compute:ctx => {ctx.set(records,'a',null);throw new Error('rollback');}},
  badQuery: {kind:'queryMethod',compute:ctx => {try{ctx.delete(records,'a')}catch{}return 1;}}
};
const http={};
for (const name of Object.keys(definitions)) {
  definitions[name].name=name;
  if(definitions[name].kind!=='derived') http[name]={name,kind:definitions[name].kind==='queryMethod'?'query':'mutation'};
}
var __flowerBundle={default:{definitions,http}};
"#;

// Reader and height records are physical structure the oracle does not keep;
// compare the logical patch, then check them against every stored cell.
fn is_reader(key: &str) -> bool {
    key.starts_with("reader:") || key.starts_with("height:")
}

fn representation(value: &Evaluation) -> Value {
    let puts: BTreeMap<_, _> = value
        .puts
        .iter()
        .filter(|(key, _)| !is_reader(key))
        .collect();
    let deletes: Vec<_> = value.deletes.iter().filter(|key| !is_reader(key)).collect();
    json!({"puts":puts,"deletes":deletes,"value":value.value,"query_cacheable":value.query_cacheable,
        "query_clock_polled":value.query_clock_polled,"query_changes_at":value.query_changes_at})
}

fn assert_readers_match_cells(data: &BTreeMap<String, Value>) {
    let expected: BTreeSet<String> = data
        .iter()
        .filter(|(key, _)| key.starts_with("cell:"))
        .flat_map(|(key, cell)| {
            cell["deps"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(move |dep| Records::reader_key(dep, key))
        })
        .collect();
    let actual: BTreeSet<String> = data
        .keys()
        .filter(|key| key.starts_with("reader:"))
        .cloned()
        .collect();
    assert_eq!(
        actual, expected,
        "reader records must mirror cell dependencies"
    );
    fn height(data: &BTreeMap<String, Value>, cell: &str) -> u64 {
        1 + data[cell]["deps"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|dep| dep.starts_with("cell:"))
            .map(|dep| height(data, dep))
            .max()
            .unwrap_or(0)
    }
    let expected: BTreeMap<String, Value> = data
        .keys()
        .filter(|key| key.starts_with("cell:"))
        .map(|cell| (Records::height_key(cell), height(data, cell)))
        .filter(|(_, height)| *height > 1)
        .map(|(key, height)| (key, json!(height)))
        .collect();
    let actual: BTreeMap<String, Value> = data
        .iter()
        .filter(|(key, _)| key.starts_with("height:"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(
        actual, expected,
        "height records must match cell dependencies"
    );
}

fn compare(data: &mut BTreeMap<String, Value>, input: Value, mode: &str, now: u64) {
    let budget = config::settings().unwrap().evaluation_timeout;
    let expected = oracle::evaluate_inner(data.clone(), input.clone(), mode, budget, Some(now));
    let actual = evaluate_inner(data.clone().into(), input.clone(), mode, budget, Some(now));
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            // Rust can evaluate one dirty child before its parent and skip an
            // unchanged parent. That diagnostic order/count is not durable
            // behavior. Still reject unexpected/additional callback execution;
            // compare every persisted dependency/outcome and returned value.
            let mut calls = BTreeMap::<&str, usize>::new();
            for id in &expected.evaluated {
                *calls.entry(id).or_default() += 1;
            }
            for id in &actual.evaluated {
                let remaining = calls.entry(id).or_default();
                assert!(*remaining > 0, "unexpected callback {id}: {mode}: {input}");
                *remaining -= 1;
            }
            assert_eq!(
                representation(&actual),
                representation(&expected),
                "{mode}: {input}"
            );
            for key in actual.deletes {
                data.remove(&key);
            }
            data.extend(actual.puts);
            assert_readers_match_cells(data);
        }
        (Err(actual), Err(expected)) => {
            assert_eq!(
                actual.to_string().split(':').next(),
                expected.to_string().split(':').next(),
                "{mode}: {input}; actual={actual}; expected={expected}"
            );
        }
        (actual, expected) => panic!("{mode}: {input}; actual={actual:?}; expected={expected:?}"),
    }
}

#[test]
fn rust_and_javascript_coordinators_agree_on_stateful_sequences() {
    warmup().unwrap();
    for marker in ["", "/* flower:static-init */\n"] {
        let javascript = format!("{marker}{APPLICATION}");
        let bundle = json!({"hash":hash(javascript.as_bytes()),"javascript":javascript});
        let mut data = BTreeMap::new();
        compare(
            &mut data,
            json!({"requestId":"deploy","bundle":bundle,
                "writes":[{"collection":"records","key":"a","value":{"key":"a","group":"x","amount":2}}],
                "materialize":[{"name":"total","args":"x"},{"name":"switched"},{"name":"clock"}]
            }),
            "deployment",
            1000,
        );
        for index in 0..24 {
            let key = ["a", "b", "東京", "😀", "\u{e000}"][index % 5];
            let group = if index % 3 == 0 { "y" } else { "x" };
            compare(
                &mut data,
                json!({"name":"stage","requestId":format!("m{index}"),"args":{
                    "key":key,"group":group,"amount":index,"keep":index%4!=0
                }}),
                "mutation",
                1100 + index as u64,
            );
            compare(
                &mut data,
                json!({"name":"read","args":{"name":"total","args":group}}),
                "query",
                1200 + index as u64,
            );
            compare(
                &mut data,
                json!({"requestId":format!("d{index}"),"writes":[
                    {"collection":"config","key":"selected","value":key}
                ]}),
                "deployment",
                1300 + index as u64,
            );
        }
        compare(&mut data, json!({"name":"scan"}), "query", 2000);
        compare(
            &mut data,
            json!({"name":"reject","requestId":"reject"}),
            "mutation",
            2001,
        );
        compare(&mut data, json!({"name":"badQuery"}), "query", 2002);
        compare(
            &mut data,
            json!({"requestId":"fail","writes":[{"collection":"records","key":"a","value":{"key":"a","group":"x","amount":1,"fail":true}}]}),
            "deployment",
            2003,
        );
        compare(
            &mut data,
            json!({"requestId":"repair","writes":[{"collection":"records","key":"a","value":{"key":"a","group":"x","amount":7}}]}),
            "deployment",
            2004,
        );
        compare(
            &mut data,
            json!({"requestId":"gc","unmaterialize":[{"name":"total","args":"x"},{"name":"total","args":"y"},{"name":"switched"},{"name":"clock"}]}),
            "deployment",
            2005,
        );
        assert!(!data.keys().any(|key| key.starts_with("cell:")));
    }
}

#[test]
fn rust_and_javascript_coordinators_agree_on_numeric_wire_values_and_identifiers() {
    warmup().unwrap();
    let mut data = BTreeMap::new();
    for (index, value) in [
        json!(1_000_000_000_000_000_128_u64),
        json!(9_007_199_254_740_993_u64),
        json!(u64::MAX),
        json!(-0.0),
        json!(1e-7),
        json!(1e21),
        json!(-1e20),
    ]
    .into_iter()
    .enumerate()
    {
        compare(
            &mut data,
            json!({"requestId":format!("number-{index}"),
                "writes":[{"collection":"numbers","key":index.to_string(),"value":{"number":value}}]
            }),
            "deployment",
            1000 + index as u64,
        );
    }
}
