use super::*;

const GENERATION: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn bundle(multiplier: u64, fail: bool) -> Value {
    let code = format!(
        r#"
const rows={{kind:'collection',name:'rows'}};
var __flowerBundle={{default:{{
 collections:[{{name:'rows',indexes:{{group:['group']}}}}],
 definitions:{{
  value:{{kind:'derived',name:'value',compute:(ctx,key)=>{{
   const row=ctx.get(rows,key);
   if ({fail} && row && row.bad) return ctx.get({{kind:'derived',name:'value'}},key);
   return row ? row.amount*{multiplier} : 0;
  }}}},
  total:{{kind:'derived',name:'total',aggregate:{{collection:'rows',fields:['group']}},compute:(_,delta)=>{{
   let total=delta.initialize?0:delta.previous;
   for (const row of delta.changes){{
    if(row.old)total-=row.old.amount*{multiplier};
    if(row.new)total+=row.new.amount*{multiplier};
   }}
   return total;
  }}}},
  time:{{kind:'derived',name:'time',compute:ctx=>ctx.now()+{multiplier}}},
  write:{{kind:'mutationMethod',name:'write',compute:(ctx,args)=>{{
   for(const row of args.rows||[]){{if(row.remove)ctx.delete(rows,row.key);else ctx.set(rows,row.key,row.value);}}
   for(const root of args.add||[])ctx.materialize({{kind:'derived',name:root.name}},root.args);
   for(const root of args.remove||[])ctx.unmaterialize({{kind:'derived',name:root.name}},root.args);
   return 'accepted';
  }}}},
  read:{{kind:'queryMethod',name:'read',compute:(ctx,args)=>ctx.get({{kind:'derived',name:args.name}},args.args)}}
 }},http:{{write:{{name:'write',kind:'mutation'}},read:{{name:'read',kind:'query'}}}}
}}}};
"#
    );
    json!({"hash":hash(code.as_bytes()),"javascript":code})
}

fn apply(data: &mut Records, evaluation: Evaluation) {
    let activation = evaluation.puts.contains_key(ACTIVE);
    for (key, value) in evaluation.puts {
        data.insert(key, value);
    }
    for key in evaluation.deletes {
        data.remove(&key);
    }
    if activation {
        // The staged service commits its terminal job alongside activation.
        let mut job = data[JOB].clone();
        job["phase"] = json!("active");
        data.insert(JOB.into(), job);
    }
}

fn reference(name: &str, args: &str) -> Value {
    json!({"name":name,"args":args})
}

fn cell(data: &Records, name: &str, args: &str) -> Value {
    data[&format!("cell:{}", json!([name, args]))]["outcome"]["value"].clone()
}

fn initial(roots: Vec<Value>) -> Records {
    let mut data = Records::new();
    let evaluation = evaluate_at(
        data.clone(),
        json!({"requestId":"initial","bundle":bundle(1,false),"materialize":roots,
        "writes":[
            {"collection":"rows","key":"a","value":{"group":"g","amount":2}},
            {"collection":"rows","key":"b","value":{"group":"g","amount":3}}
        ]}),
        100,
    )
    .unwrap();
    apply(&mut data, evaluation);
    data
}

fn stage(data: &mut Records, target: Value) -> Value {
    let input = json!({"requestId":"staged","bundle":target});
    data.insert(
        JOB.into(),
        json!({"requestId":"staged","phase":"rebuilding","generation":GENERATION,"bundleHash":input["bundle"]["hash"]}),
    );
    data.insert(PLAN.into(), json!({"bundle":input["bundle"]}));
    data.insert(INDEXES.into(), json!({"indexes":[],"aggregates":{}}));
    input
}

fn ready(data: &mut Records) {
    let mut job = data[JOB].clone();
    job["phase"] = json!("ready");
    data.insert(JOB.into(), job);
}

#[test]
fn root_pages_are_hidden_reuse_completed_cells_and_cut_over_atomically() {
    let a = reference("value", "a");
    let b = reference("value", "b");
    let mut data = initial(vec![a.clone(), b.clone()]);
    let input = stage(&mut data, bundle(10, false));
    let first = graph_page(data.clone(), input.clone(), GENERATION, vec![a], 100).unwrap();
    assert!(first.puts.keys().all(|key| key.starts_with("graph:")));
    apply(&mut data, first);
    assert_eq!(cell(&data, "value", "a"), 2);
    assert_eq!(cell(&data.graph_view(Some(GENERATION)), "value", "a"), 20);
    let second = graph_page(data.clone(), input.clone(), GENERATION, vec![b], 100).unwrap();
    assert!(!second
        .evaluated
        .iter()
        .any(|key| key == "cell:[\"value\",\"a\"]"));
    apply(&mut data, second);
    ready(&mut data);
    let activation = activate(data.clone(), input, 100).unwrap();
    assert!(
        activation.evaluated.is_empty(),
        "cutover must reuse the completed graph"
    );
    assert_eq!(activation.puts[ACTIVE], GENERATION);
    apply(&mut data, activation);
    assert_eq!(cell(&data, "value", "a"), 20);
    assert_eq!(cell(&data, "value", "b"), 30);
    let restored: Records = serde_json::from_str(&serde_json::to_string(&data).unwrap()).unwrap();
    assert_eq!(cell(&restored, "value", "a"), 20);
    let changed=invoke_at(restored.clone(),json!({"name":"write","requestId":"after","args":{"rows":[{"key":"a","value":{"group":"g","amount":4}}]}}),"mutation",101).unwrap();
    assert!(changed
        .puts
        .contains_key(&format!("graph:{GENERATION}:cell:[\"value\",\"a\"]")));
    let mut continued = restored;
    apply(&mut continued, changed);
    assert_eq!(cell(&continued, "value", "a"), 40);
    let mut collected = continued[JOB].clone();
    collected["phase"] = json!("collected");
    continued.insert(JOB.into(), collected);
    let redeployed = evaluate_at(
        continued.clone(),
        json!({"requestId":"direct-after-staged","bundle":bundle(100,false)}),
        102,
    )
    .unwrap();
    apply(&mut continued, redeployed);
    assert_eq!(cell(&continued, "value", "a"), 400);
    assert_eq!(
        continued.get_raw_shared("cell:[\"value\",\"a\"]").unwrap()["outcome"]["value"],
        2,
        "direct deployment must not overwrite the retired graph"
    );
}

#[test]
fn dual_updates_replay_final_source_deltas_and_track_root_membership() {
    let total = reference("total", "g");
    let a = reference("value", "a");
    let mut data = initial(vec![total.clone(), a.clone()]);
    let input = stage(&mut data, bundle(10, false));
    let page = graph_page(
        data.clone(),
        input.clone(),
        GENERATION,
        vec![total, a.clone()],
        100,
    )
    .unwrap();
    apply(&mut data, page);
    let mutation=invoke_speculative_as(data.clone(),json!({"name":"write","requestId":"change","args":{
        "rows":[{"key":"a","value":{"group":"g","amount":7}},{"key":"a","value":{"group":"g","amount":9}},
            {"key":"b","remove":true},{"key":"0","value":{"group":"g","amount":4}}],
        "add":[reference("value","0")],"remove":[a]
    }}),101,Value::Null).unwrap();
    assert!(mutation.mutation_certificate.is_none());
    apply(&mut data, mutation);
    assert_eq!(cell(&data, "total", "g"), 13);
    let shadow = data.graph_view(Some(GENERATION));
    assert_eq!(cell(&shadow, "total", "g"), 130);
    assert_eq!(cell(&shadow, "value", "0"), 40);
    assert!(!shadow.contains_key("root:[\"value\",\"a\"]"));
    assert!(!shadow.contains_key("cell:[\"value\",\"a\"]"));
    ready(&mut data);
    let activation = activate(data.clone(), input, 101).unwrap();
    apply(&mut data, activation);
    assert_eq!(cell(&data, "total", "g"), 130);
}

#[test]
fn shadow_failure_preserves_active_write_and_fences_activation() {
    let root = reference("value", "a");
    let mut data = initial(vec![root.clone()]);
    let input = stage(&mut data, bundle(10, true));
    let page = graph_page(data.clone(), input.clone(), GENERATION, vec![root], 100).unwrap();
    apply(&mut data, page);
    ready(&mut data);
    let mutation=invoke_at(data.clone(),json!({"name":"write","requestId":"bad-shadow","args":{"rows":[{"key":"a","value":{"group":"g","amount":8,"bad":true}}]}}),"mutation",101).unwrap();
    assert_eq!(mutation.value, "accepted");
    assert_eq!(mutation.puts[JOB]["phase"], "failed");
    assert!(mutation.puts[JOB]["error"]
        .as_str()
        .unwrap()
        .contains("CYCLE"));
    apply(&mut data, mutation);
    assert_eq!(cell(&data, "value", "a"), 8);
    assert_eq!(cell(&data.graph_view(Some(GENERATION)), "value", "a"), 20);
    assert!(activate(data, input, 101)
        .unwrap_err()
        .to_string()
        .contains("not ready"));
}

#[test]
fn graph_clocks_stay_private_until_activation_and_refresh_at_cutover() {
    let root = reference("time", "unused");
    let mut data = initial(vec![root.clone()]);
    let input = stage(&mut data, bundle(10, false));
    let page = graph_page(data.clone(), input.clone(), GENERATION, vec![root], 200).unwrap();
    apply(&mut data, page);
    assert_eq!(data["clock"], 100);
    assert_eq!(cell(&data, "time", "unused"), 101);
    assert_eq!(
        cell(&data.graph_view(Some(GENERATION)), "time", "unused"),
        210
    );
    // A restart or clock rollback must not let the two graphs use different
    // effective times when the shadow's last page advanced farther.
    let mutation = invoke_at(
        data.clone(),
        json!({"name":"write","requestId":"rolled-clock","args":{}}),
        "mutation",
        150,
    )
    .unwrap();
    apply(&mut data, mutation);
    assert_eq!(data["clock"], 200);
    assert_eq!(cell(&data, "time", "unused"), 201);
    assert_eq!(
        cell(&data.graph_view(Some(GENERATION)), "time", "unused"),
        210
    );
    ready(&mut data);
    let activation = activate(data.clone(), input, 300).unwrap();
    apply(&mut data, activation);
    assert_eq!(data["clock"], 300);
    assert_eq!(cell(&data, "time", "unused"), 310);
}

#[test]
fn staging_fences_prior_speculation_and_malformed_graph_pointers() {
    let mut data = initial(vec![reference("value", "a")]);
    let prepared = invoke_speculative_as(
        data.clone(),
        json!({"name":"write","requestId":"prepared","args":{}}),
        100,
        Value::Null,
    )
    .unwrap();
    let certificate = prepared.mutation_certificate.unwrap();
    assert!(certificate.valid(&data));
    stage(&mut data, bundle(10, false));
    assert!(!certificate.valid(&data));
    data.insert(ACTIVE.into(), json!("malformed"));
    assert!(!certificate.valid(&data));
    let error = invoke_at(
        data,
        json!({"name":"read","args":reference("value","a")}),
        "query",
        100,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("Malformed active reactive graph pointer"));
}

#[test]
fn key_policy_updates_maintain_shadow_roots_and_activation_checks_readiness() {
    let code = r#"
const key={kind:'key',name:'sessions',algorithm:'Ed25519',usages:['publicKey']};
var __flowerBundle={default:{keys:[key],definitions:{
 value:{kind:'derived',name:'value',compute:()=>__flowerCrypto(200,0,JSON.stringify({operation:'key.publicKey',key}),'','','')}
},http:{}}};
"#;
    let root = reference("value", "a");
    let mut data = initial(vec![root.clone()]);
    let input = stage(
        &mut data,
        json!({"hash":hash(code.as_bytes()),"javascript":code}),
    );
    let page = graph_page(data.clone(), input.clone(), GENERATION, vec![root], 100).unwrap();
    apply(&mut data, page);
    ready(&mut data);
    let shadow = data.graph_view(Some(GENERATION));
    assert_eq!(
        shadow["cell:[\"value\",\"a\"]"]["deps"],
        json!(["managedKeys"])
    );
    assert!(activate(data.clone(), input.clone(), 100)
        .unwrap_err()
        .to_string()
        .contains("KEY_UNAVAILABLE"));
    let catalog = json!({"domain":"staging-tests","revision":1,"keys":{},"bindings":{}});
    let update = update_keys_at(data.clone(), catalog.clone(), 101).unwrap();
    assert_eq!(update.puts["managedKeys"], catalog);
    assert!(update
        .puts
        .contains_key(&format!("graph:{GENERATION}:cell:[\"value\",\"a\"]")));
    assert!(
        !update.puts.contains_key(JOB),
        "a valid policy update must not fail the build"
    );
    apply(&mut data, update);
    assert_eq!(cell(&data, "value", "a"), 2);
    let activation = activate(data.clone(), input, 101).unwrap();
    apply(&mut data, activation);
    assert_eq!(data["keyDeclarations"][0]["name"], "sessions");
    assert_eq!(data.active_graph(), Some(GENERATION));
}
