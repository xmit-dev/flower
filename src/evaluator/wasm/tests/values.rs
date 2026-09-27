//! Values crossing the guest boundary: the native codec reads QuickJS shapes,
//! arrays and strings directly and never runs application code while encoding.
use super::*;

const PLAIN: &str =
    "values must be null, booleans, finite numbers, strings, arrays or plain objects";
const HIDDEN: &str = "values cannot contain symbols, hidden properties or accessors";
const FINITE: &str = "numbers must be finite";
const CYCLES: &str = "values cannot contain cycles";
const HOLES: &str = "arrays cannot contain holes";
const NAMED: &str = "arrays cannot contain named properties";
const DEPTH: &str = "values nest at most 128 levels";

fn ok(value: Value) -> Value {
    json!({"ok": true, "value": value})
}

fn failed(code: &str, message: &str) -> Value {
    json!({"ok": false, "error": {"code": code, "message": message}})
}

fn invoke(source: &str, host: Value) -> (Value, Vec<(String, Value)>) {
    let mut calls = Vec::new();
    let result = execute(
        source,
        "test",
        &Value::Null,
        "query",
        &mut |method, args| {
            calls.push((method.to_owned(), args));
            Ok(host.clone())
        },
        limits(),
    )
    .unwrap();
    (result, calls)
}

#[test]
fn results_are_plain_data_read_without_running_application_code() {
    let nested = |levels: usize| {
        let mut value = Value::Null;
        for _ in 0..levels {
            value = json!([value]);
        }
        value
    };
    let many_keys: Vec<Value> = (0..600)
        .map(|index| json!({format!("k{index}"): index, "shared": index}))
        .collect();
    for (prefix, compute, expected) in [
        (
            "",
            "()=>[null,true,'字\\u0000🌻',3.14,-0]",
            ok(json!([null, true, "字\0🌻", 3.14, 0])),
        ),
        (
            "",
            "()=>[2147483647,2147483648,-2147483649,2**53-1,2**53,1e21,0.5,4/2]",
            ok(json!([
                2147483647,
                2147483648_u64,
                -2147483649_i64,
                9007199254740991_u64,
                9007199254740992_u64,
                1e21,
                0.5,
                2
            ])),
        ),
        ("", "()=>Infinity", failed("INVALID_VALUE", FINITE)),
        ("", "()=>NaN", failed("INVALID_VALUE", FINITE)),
        (
            "",
            "()=>{Number.isFinite=()=>true;return Infinity}",
            failed("INVALID_VALUE", FINITE),
        ),
        (
            "let Number={isFinite:()=>true};",
            "()=>-Infinity",
            failed("INVALID_VALUE", FINITE),
        ),
        ("", "()=>undefined", failed("INVALID_VALUE", PLAIN)),
        ("", "()=>[1,undefined,3]", failed("INVALID_VALUE", PLAIN)),
        ("", "()=>()=>1", failed("INVALID_VALUE", PLAIN)),
        ("", "()=>Symbol('x')", failed("INVALID_VALUE", PLAIN)),
        ("", "()=>1n", failed("INVALID_VALUE", PLAIN)),
        (
            "",
            "()=>Object.create({custom:true})",
            failed("INVALID_VALUE", PLAIN),
        ),
        ("", "()=>new Map([['x',1]])", failed("INVALID_VALUE", PLAIN)),
        (
            "",
            "()=>{const x=new Number(3);Object.setPrototypeOf(x,Object.prototype);return x}",
            failed("INVALID_VALUE", PLAIN),
        ),
        (
            "",
            "()=>new Proxy({x:1},{})",
            failed("INVALID_VALUE", PLAIN),
        ),
        ("", "()=>new Proxy([1],{})", failed("INVALID_VALUE", PLAIN)),
        (
            "",
            "()=>Object.defineProperty({x:1},'hidden',{value:2})",
            failed("INVALID_VALUE", HIDDEN),
        ),
        (
            "",
            "()=>({[Symbol('x')]:1})",
            failed("INVALID_VALUE", HIDDEN),
        ),
        (
            "",
            "()=>{const x={};x.self=x;return x}",
            failed("INVALID_VALUE", CYCLES),
        ),
        (
            "",
            "()=>{const x=[1];x.push(x);return x}",
            failed("INVALID_VALUE", CYCLES),
        ),
        ("", "()=>[1,,3]", failed("INVALID_VALUE", HOLES)),
        (
            "",
            "()=>{const x=[1,2];x.length=4;return x}",
            failed("INVALID_VALUE", HOLES),
        ),
        (
            "",
            "()=>{const x=[1,2,3];delete x[1];return x}",
            failed("INVALID_VALUE", HOLES),
        ),
        (
            "",
            "()=>Object.assign([1],{extra:()=>1})",
            failed("INVALID_VALUE", NAMED),
        ),
        (
            "",
            "()=>Object.defineProperty([1],'extra',{value:2})",
            failed("INVALID_VALUE", NAMED),
        ),
        (
            "",
            "()=>{const x=[1];x[Symbol('extra')]=2;return x}",
            failed("INVALID_VALUE", NAMED),
        ),
        ("", "()=>'x'.match(/x/)", failed("INVALID_VALUE", NAMED)),
        (
            "",
            "()=>{const shared={n:[1]};return [shared,{shared},shared]}",
            ok(json!([{"n":[1]},{"shared":{"n":[1]}},{"n":[1]}])),
        ),
        (
            "",
            "()=>{Object.prototype.toJSON=()=>'converted';return {tag:'ok'}}",
            ok(json!({"tag":"ok"})),
        ),
        (
            "",
            "()=>{Array.isArray=()=>false;return [1,2]}",
            ok(json!([1, 2])),
        ),
        (
            "",
            "()=>{const x={first:1,gone:2,last:3};delete x.gone;x.again={ok:true};return x}",
            ok(json!({"first":1,"last":3,"again":{"ok":true}})),
        ),
        (
            "",
            "()=>{const x={9:'nine',2:'two',text:1};delete x[2];x[2]='again';return x}",
            ok(json!({"2":"again","9":"nine","text":1})),
        ),
        (
            "",
            "()=>Object.freeze({x:1,nested:Object.freeze([1,2])})",
            ok(json!({"x":1,"nested":[1,2]})),
        ),
        ("", "()=>Object.seal([1,{x:2}])", ok(json!([1, {"x":2}]))),
        (
            "",
            "()=>{const x=[1,{x:2}];Object.setPrototypeOf(x,null);return x}",
            ok(json!([1, {"x":2}])),
        ),
        (
            "",
            "()=>{const x=[1,2,3];x.length=1;return x}",
            ok(json!([1])),
        ),
        (
            "",
            "()=>{const x=[1,2,3];delete x[1];x[1]=4;return x}",
            ok(json!([1, 4, 3])),
        ),
        (
            "",
            "()=>Object.assign(Object.create(null),{x:1})",
            ok(json!({"x":1})),
        ),
        (
            "",
            "()=>({'é':1,'😀':2,'字':3,2:'b',10:'c'})",
            ok(json!({"é":1,"😀":2,"字":3,"2":"b","10":"c"})),
        ),
        (
            "",
            "()=>{const a='a'.repeat(700),b='é'.repeat(700),c='😀'.repeat(300);return [a+b,a+c,(a+b+c).length]}",
            ok(json!([
                format!("{}{}", "a".repeat(700), "é".repeat(700)),
                format!("{}{}", "a".repeat(700), "😀".repeat(300)),
                2000
            ])),
        ),
        (
            "",
            "()=>['0123456789'.repeat(500).slice(3,4003),'😀'.repeat(1000).slice(2,1602)]",
            ok(json!(["0123456789".repeat(500)[3..4003], "😀".repeat(800)])),
        ),
        (
            "",
            "()=>Array.from({length:600},(_,i)=>({['k'+i]:i,shared:i}))",
            ok(Value::Array(many_keys)),
        ),
        (
            "",
            "()=>{let x=null;for(let i=0;i<127;i++)x=[x];return x}",
            ok(nested(127)),
        ),
        (
            "",
            "()=>{let x=null;for(let i=0;i<128;i++)x=[x];return x}",
            failed("INVALID_VALUE", DEPTH),
        ),
        (
            "",
            "()=>'\\ud800'",
            failed("INVALID_VALUE", "string has a lone surrogate"),
        ),
    ] {
        let source = format!("{prefix}{}", bundle(compute, false));
        assert_eq!(
            invoke(&source, Value::Null),
            (expected, Vec::new()),
            "{compute}"
        );
    }
}

#[test]
fn hooks_on_results_and_arguments_never_run() {
    for (compute, expected) in [
        (
            "ctx=>({get forbidden(){ctx.now();return 1}})",
            failed("INVALID_VALUE", HIDDEN),
        ),
        (
            "ctx=>{const x=[1];Object.defineProperty(x,'extra',{enumerable:true,get(){ctx.now();return 2}});return x}",
            failed("INVALID_VALUE", NAMED),
        ),
        (
            "ctx=>{const x={};x[9]=new Proxy({x:9},{ownKeys(t){ctx.get('trace','nine');return Reflect.ownKeys(t)}});return x}",
            failed("INVALID_VALUE", PLAIN),
        ),
        (
            "ctx=>ctx.get('items',Object.assign([1],{extra:{valid:true}}))",
            failed("INVALID_VALUE", NAMED),
        ),
        (
            "ctx=>{const key=[1];key.length=3;try{return ctx.get('items',key)}catch(e){return [e.code,e.message]}}",
            ok(json!(["INVALID_VALUE", HOLES])),
        ),
        (
            "ctx=>{try{ctx.get({kind:'collection',name:'x'},{get key(){ctx.now();return 1}})}catch(e){return [e.code,e.message]}}",
            ok(json!(["INVALID_VALUE", HIDDEN])),
        ),
        (
            "ctx=>{try{ctx.set('records','key',[1,,3])}catch(e){return [e.code,e.message]}}",
            ok(json!(["INVALID_VALUE", HOLES])),
        ),
        (
            "ctx=>{Array.prototype.toJSON=()=>{ctx.now();return 'converted'};return ctx.get('items',[1])}",
            ok(Value::Null),
        ),
    ] {
        let (result, calls) = invoke(&bundle(compute, false), Value::Null);
        assert_eq!(result, expected, "{compute}");
        assert!(
            calls.iter().all(|(method, _)| method == "get"),
            "{compute}: {calls:?}"
        );
    }
}

#[test]
fn host_calls_exchange_values_and_failures() {
    let (result, calls) = invoke(
        &bundle(
            "ctx=>{const v=ctx.get('items',{a:[1,'é',{b:null}],'😀':-1.5});return [v,Object.keys(v),v.nested.é]}",
            false,
        ),
        json!({"b":1,"a":[true,{"é":"😀"}],"n":2.5,"10":0,"2":0,"nested":{"é":"字"}}),
    );
    assert_eq!(
        calls,
        [(
            "get".into(),
            json!(["items", {"a":[1,"é",{"b":null}],"😀":-1.5}])
        )]
    );
    assert_eq!(
        result,
        ok(json!([
            {"b":1,"a":[true,{"é":"😀"}],"n":2.5,"10":0,"2":0,"nested":{"é":"字"}},
            ["2", "10", "a", "b", "n", "nested"],
            "字"
        ]))
    );
    // Lone surrogates are the caller's error; the database never sees them.
    let (result, calls) = invoke(
        &bundle(
            "ctx=>{try{return ctx.get('items','\\ud800')}catch(e){return [e.code,e.message]}}",
            false,
        ),
        Value::Null,
    );
    assert_eq!(
        result,
        ok(json!(["INVALID_VALUE", "string has a lone surrogate"]))
    );
    assert!(calls.is_empty());
    let result = execute(
        &bundle(
            "ctx=>{try{ctx.get('x','y')}catch(e){return [e.code,e.message,e instanceof Error,typeof e.stack,e.details===undefined]}}",
            false,
        ),
        "test",
        &Value::Null,
        "query",
        &mut |_, _| {
            let mut error = crate::evaluator::rust_engine::EngineError::new("NOPE", "no 🌸");
            error.details = Some(json!({"hidden": true}));
            Err(anyhow::Error::new(error))
        },
        limits(),
    )
    .unwrap();
    assert_eq!(result, ok(json!(["NOPE", "no 🌸", true, "string", true])));
}

#[test]
fn callback_failures_carry_codes_messages_and_representable_details() {
    for (compute, expected) in [
        (
            "()=>{throw Object.assign(new Error('boom'),{code:'CUSTOM',details:{x:[1]}})}",
            json!({"ok":false,"error":{"code":"CUSTOM","message":"boom","details":{"x":[1]}}}),
        ),
        (
            "()=>{throw Object.assign(new Error('boom'),{details:()=>1})}",
            failed("COMPUTE_ERROR", "boom"),
        ),
        (
            "()=>{throw {message:'plain',get details(){throw 1}}}",
            failed("COMPUTE_ERROR", "plain"),
        ),
        ("()=>{throw 'text'}", failed("COMPUTE_ERROR", "text")),
        (
            "()=>{throw Object.assign(new Error('x'),{code:7})}",
            failed("COMPUTE_ERROR", "x"),
        ),
        (
            "()=>{throw new Error('out of memory')}",
            failed("EVALUATION_BUDGET", "out of memory"),
        ),
        (
            "()=>{throw {get message(){throw Error('conversion 🌸\\0tail')}}}",
            failed("EVALUATION_BUDGET", "Error: conversion 🌸\0tail"),
        ),
        (
            "()=>{throw {get message(){throw {toString(){throw null}}}}}",
            failed("EVALUATION_BUDGET", "unreadable QuickJS exception"),
        ),
    ] {
        assert_eq!(
            invoke(&bundle(compute, false), Value::Null).0,
            expected,
            "{compute}"
        );
    }
    // Derived values never carry details.
    let source = "var __flowerBundle={default:{definitions:{d:{name:'d',kind:'derived',compute:()=>{throw Object.assign(new Error('boom'),{code:'CUSTOM',details:1})}}},http:{}}};";
    let result = execute(
        source,
        "d",
        &Value::Null,
        "derived",
        &mut |_, _| Ok(Value::Null),
        limits(),
    )
    .unwrap();
    assert_eq!(result, failed("CUSTOM", "boom"));
}
