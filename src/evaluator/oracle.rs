//! The original JavaScript coordinator remains a differential test oracle.
//!
//! It runs inside the production QuickJS Wasm sandbox, so the test suite needs
//! no native interpreter. These trusted fixtures recreate each callback's lexical
//! scope; production global/prototype isolation is covered by the Wasm tests.
use super::*;

const ENGINE: &str = include_str!("../../runtime/engine.js");
const RUNNER: &str = include_str!("reference-cell-runner.js");

pub(super) fn evaluate_inner(
    data: BTreeMap<String, Value>,
    mutation: Value,
    mode: &str,
    timeout: Duration,
    now: Option<u64>,
) -> Result<Evaluation> {
    let bundle = mutation
        .get("bundle")
        .or_else(|| data.get("bundle"))
        .and_then(|value| value.get("javascript"))
        .and_then(Value::as_str)
        .unwrap_or("var __flowerBundle={default:{definitions:{},http:{}}};");
    let shared = wasm::Limits::new(
        Instant::now() + timeout,
        config::settings()?.guest_memory_bytes,
    );
    // The reference coordinator shares the production manifest validation.
    let manifest = match mutation.get("bundle") {
        Some(_) => {
            let manifest = wasm::manifest(bundle, shared.clone())
                .and_then(|raw| manifest::validate(&raw))
                .map_err(|error| anyhow::anyhow!("EVALUATION_ERROR: {error}"))?;
            json!({"http": manifest.http, "maintenance": manifest.maintenance, "authorize": manifest.authorize})
        }
        None => Value::Null,
    };
    let data_json = if mode == "deployment" {
        serde_json::to_string(&data)?
    } else {
        json_order::ordered_snapshot(&data, true)?
    };
    let code = format!(
        r#"
        {ENGINE}
        (() => {{
            const bundle = {bundle};
            const runner = {runner};
            const data = JSON.parse({data});
            const input = {mutation};
            const mode = {mode};
            const now = {now};
            const execute = (kind, name, args, api) => {{
                const read = (method, payload) => {{
                    try {{ return JSON.stringify({{ok:true,value:api[method](...JSON.parse(payload))}}); }}
                    catch(e) {{ return JSON.stringify({{ok:false,error:{{code:String(e.code||'COMPUTE_ERROR'),message:String(e.message||e)}}}}); }}
                }};
                const run = new Function('__name','__argsJson','__kind','__flowerRead',bundle+'\nreturn '+runner);
                const result = JSON.parse(run(name,JSON.stringify(args),kind,read));
                if(!result.ok) throw Object.assign(new Error(result.error.message),{{code:result.error.code}});
                return result.value;
            }};
            try {{
                const manifest = {manifest};
                const derived = (name,args,api) => execute('derived',name,args,api);
                if(mode==='deployment' && now!==null) input.now=now;
                const value = mode==='deployment' ? flowerEvaluate(data,input,derived) :
                    flowerInvokeOrdered(data,{{...input,kind:mode}},(name,args,api)=>execute(mode,name,args,api),derived,now===null?undefined:now);
                if(manifest) {{ value.puts.httpMethods=manifest.http; value.puts.maintenanceMethod=manifest.maintenance; value.puts.authorizationMethod=manifest.authorize; }}
                return JSON.stringify({{ok:true,value}});
            }} catch(e) {{ return JSON.stringify({{ok:false,error:{{code:String(e.code||'EVALUATION_ERROR'),message:String(e.message||e)}}}}); }}
        }})()
    "#,
        bundle = serde_json::to_string(bundle)?,
        runner = serde_json::to_string(RUNNER.trim())?,
        data = serde_json::to_string(&data_json)?,
        mutation = mutation,
        mode = serde_json::to_string(mode)?,
        now = serde_json::to_string(&now)?,
        manifest = manifest,
    );
    let envelope: Value = serde_json::from_str(&wasm::reference_script(&code, shared)?)?;
    if envelope["ok"] != true {
        anyhow::bail!(
            "{}: {}",
            envelope["error"]["code"]
                .as_str()
                .unwrap_or("EVALUATION_ERROR"),
            envelope["error"]["message"]
                .as_str()
                .unwrap_or("evaluation failed")
        );
    }
    Ok(serde_json::from_value(envelope["value"].clone())?)
}
