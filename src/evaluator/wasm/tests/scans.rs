use super::*;
use std::collections::BTreeMap;

#[test]
fn scan_bridge_preserves_options_and_index_fields_and_omits_undefined_options() {
    for static_init in [false, true] {
        let code = bundle(
            r#"ctx=>{
                const rows={kind:'collection',name:'rows',indexes:{byScore:['tenant','score']}};
                ctx.scan(rows);
                ctx.scan(rows,undefined);
                ctx.scan(rows,{index:'byScore',prefix:['a'],gte:1,lte:5,reverse:true,offset:2,limit:3});
                ctx.scan({kind:'collection',name:'rows'},{gt:'a',limit:0});
                return 'done';
            }"#,
            static_init,
        );
        let mut calls = Vec::new();
        let result = execute(
            &code,
            "test",
            &Value::Null,
            "query",
            &mut |method, args| {
                assert_eq!(method, "scan");
                calls.push(args);
                Ok(json!([]))
            },
            limits(),
        )
        .unwrap();
        assert_eq!(result, json!({"ok":true,"value":"done"}));
        assert_eq!(
            calls,
            vec![
                json!([{"kind":"collection","name":"rows"}]),
                json!([{"kind":"collection","name":"rows"}]),
                json!([
                    {"kind":"collection","name":"rows","indexes":{"byScore":["tenant","score"]}},
                    {"index":"byScore","prefix":["a"],"gte":1,"lte":5,"reverse":true,"offset":2,"limit":3}
                ]),
                json!([{"kind":"collection","name":"rows","indexes":{}},{"gt":"a","limit":0}])
            ]
        );
    }
}

#[test]
fn scan_bridge_rejects_accessors_without_invoking_them() {
    for static_init in [false, true] {
        let code = bundle(
            r#"ctx=>{
                let accesses=0;
                const rows={kind:'collection',name:'rows',indexes:{byScore:['score']}};
                const getter=()=>{++accesses;return 1};
                const prefix=[];
                Object.defineProperty(prefix,'0',{get:getter,enumerable:true});
                const badIndexes={kind:'collection',name:'rows',get indexes(){return getter()}};
                const errors=[];
                for(const [ref,options] of [
                    [rows,{get limit(){return getter()}}],
                    [rows,{index:'byScore',prefix}],
                    [badIndexes,{index:'byScore'}]
                ]) {
                    try {ctx.scan(ref,options)} catch(error) {errors.push(error.code)}
                }
                return {accesses,errors};
            }"#,
            static_init,
        );
        let result = execute(
            &code,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| panic!("Invalid descriptors must not reach the host"),
            limits(),
        )
        .unwrap();
        assert_eq!(
            result,
            json!({"ok":true,"value":{"accesses":0,"errors":["INVALID_VALUE","INVALID_VALUE","INVALID_VALUE"]}})
        );
    }
}

#[test]
fn wasm_scans_apply_ordered_windows_and_refresh_derived_reads_after_staged_writes() {
    use crate::consensus::Records;
    use crate::evaluator::{evaluate, hash, invoke_at};

    // Exercise both persistent declared indexes and the undeclared-index fallback.
    for declared in [false, true] {
        let javascript = format!(
            r#"const rows={{kind:'collection',name:'rows',indexes:{{byScore:['tenant','score']}}}};
            const options={{index:'byScore',prefix:['a'],gte:0,reverse:true,offset:1,limit:1}};
            var __flowerBundle={{default:{{collections:{collections},definitions:{{
                page:{{kind:'derived',name:'page',compute:ctx=>ctx.scan(rows,options)}},
                save:{{kind:'mutationMethod',name:'save',compute:ctx=>{{
                    ctx.set(rows,'a',{{tenant:'a',score:4}});
                    return {{direct:ctx.scan(rows,options),derived:ctx.get({{kind:'derived',name:'page'}})}};
                }}}},
                read:{{kind:'queryMethod',name:'read',compute:(ctx,args)=>ctx.scan(rows,args)}}
            }},http:{{save:{{kind:'mutation',name:'save'}},read:{{kind:'query',name:'read'}}}}}}}};"#,
            collections = if declared {
                "[{name:rows.name,indexes:rows.indexes}]"
            } else {
                "[]"
            },
        );
        let deployed = evaluate(
            BTreeMap::new(),
            json!({
                "requestId":"scan-deploy",
                "bundle":{"hash":hash(javascript.as_bytes()),"javascript":javascript},
                "writes":[
                    {"collection":"rows","key":"a","value":{"tenant":"a","score":1}},
                    {"collection":"rows","key":"b","value":{"tenant":"a","score":2}},
                    {"collection":"rows","key":"c","value":{"tenant":"a","score":3}},
                    {"collection":"rows","key":"elsewhere","value":{"tenant":"b","score":2}}
                ],
                "materialize":[{"name":"page"}]
            }),
        )
        .unwrap();
        let mut data: Records = deployed.puts.into();
        assert_eq!(
            data["cell:[\"page\",null]"]["outcome"]["value"],
            json!([{"key":"b","value":{"tenant":"a","score":2}}])
        );
        let result = invoke_at(
            data.clone(),
            json!({"name":"save","requestId":"scan-save"}),
            "mutation",
            100,
        )
        .unwrap();
        let expected = json!([{"key":"c","value":{"tenant":"a","score":3}}]);
        assert_eq!(result.value, json!({"direct":expected,"derived":expected}));
        for (id, value) in result.puts {
            data.insert(id, value);
        }
        for id in result.deletes {
            data.remove(&id);
        }
        assert_eq!(data["cell:[\"page\",null]"]["outcome"]["value"], expected);
        let result = invoke_at(
            data.clone(),
            json!({"name":"read","args":{"gte":"a","lte":"c","reverse":true,"offset":1,"limit":1}}),
            "query",
            100,
        )
        .unwrap();
        assert_eq!(
            result.value,
            json!([{"key":"b","value":{"tenant":"a","score":2}}])
        );
        let error = invoke_at(
            data,
            json!({"name":"read","args":{"index":"unknown"}}),
            "query",
            100,
        )
        .unwrap_err();
        assert!(error.to_string().contains("INVALID_REFERENCE"), "{error:#}");
    }
}
