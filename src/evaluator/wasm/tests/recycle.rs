use super::super::recycle as pool;
use super::*;

#[test]
fn recycled_guests_restore_globals_closures_typed_arrays_prototypes_and_host_callbacks() {
    for static_init in [false, true] {
        let source = format!(
            "{}let count=0;const bytes=new Uint8Array([7,8]);const fn=()=>1;globalThis.box={{n:0,fn}};{}",
            if static_init { STATIC_INIT_MARKER } else { "" },
            bundle(
                r#"(ctx,args)=>{
                const prior=[++count,++box.n,bytes[0],Object.prototype.leak||null,
                    Array.prototype.leak||null,Function.prototype.leak||null,fn===box.fn,fn.calls||0];
                bytes[0]=99;Object.prototype.leak=1;Array.prototype.leak=2;
                Function.prototype.leak=3;fn.calls=4;box.fn=()=>2;
                return {prior,args,host:ctx.get('fresh','callback')};
            }"#,
                false
            )
        );
        let prepared = prepare(&source, limits()).unwrap();
        for (index, args) in [
            json!({"text":"long 🌺".repeat(100)}),
            json!("short\0"),
            json!({"large":"payload".repeat(2048)}),
            Value::Null,
        ]
        .into_iter()
        .enumerate()
        {
            let result = execute_prepared(
                &prepared,
                "test",
                &args,
                "query",
                &mut |_, _| Ok(json!(index)),
                limits(),
            )
            .unwrap();
            assert_eq!(
                result,
                json!({"ok":true,"value":{
                "prior":[1,1,7,null,null,null,true,0],"args":args,"host":index}})
            );
        }
        assert!(
            pool::stats(&prepared).reused >= 1,
            "the isolation assertions must exercise Store reuse"
        );
    }
}

#[test]
fn concurrent_cold_bundle_callers_keep_heaps_callbacks_and_budgets_private() {
    warmup().unwrap();
    for static_init in [false, true] {
        let source = format!(
            "{}let count=0;globalThis.box={{n:0}};{}",
            if static_init { STATIC_INIT_MARKER } else { "" },
            bundle(
                "(ctx,args)=>{const prior=Object.prototype.concurrentLeak||null;Object.prototype.concurrentLeak=args.worker+1;return[++count,++box.n,prior,args,ctx.get('concurrent-cold','value')]}",
                false
            )
        );
        let start = std::sync::Barrier::new(4);
        let prepared = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let callers = (0..4)
                .map(|worker| {
                    let (source, start, prepared) = (&source, &start, &prepared);
                    scope.spawn(move || {
                        start.wait();
                        let image = prepare(source, limits());
                        prepared.wait();
                        let image = image.unwrap();
                        if worker == 0 {
                            // Fail one invocation after image preparation. Its
                            // sticky memory failure must not taint any other
                            // caller sharing the immutable prepared image.
                            let failed = Limits::new(Instant::now() + Duration::from_secs(30), 1);
                            assert!(execute_prepared(
                                &image,
                                "test",
                                &Value::Null,
                                "query",
                                &mut |_, _| panic!("memory failure must precede callbacks"),
                                failed.clone(),
                            )
                            .is_err());
                            assert!(failed.check().is_err());
                        }
                        for round in 0..2 {
                            let args = json!({"worker":worker,"round":round});
                            let result = execute_prepared(
                                &image,
                                "test",
                                &args,
                                "query",
                                &mut |method, input| {
                                    assert_eq!(method, "get");
                                    assert_eq!(input, json!(["concurrent-cold", "value"]));
                                    Ok(json!([worker, round]))
                                },
                                limits(),
                            )
                            .unwrap();
                            assert_eq!(
                                result,
                                json!({"ok":true,"value":[1,1,null,args,[worker,round]]})
                            );
                        }
                        let stats = pool::stats(&image);
                        assert!(
                            stats.reused >= 1,
                            "static_init={static_init} worker={worker} {stats:?}"
                        );
                    })
                })
                .collect::<Vec<_>>();
            for caller in callers {
                caller.join().unwrap();
            }
        });
    }
}

#[test]
fn nested_callbacks_on_the_same_image_keep_distinct_active_heaps_and_bridges() {
    let source = format!(
        "{STATIC_INIT_MARKER}let count=0;{}",
        bundle(
            "(ctx,args)=>{++count;const value=ctx.get(args.nested?'nested':'leaf',null);return[count,value,count]}",
            false
        )
    );
    let prepared = prepare(&source, limits()).unwrap();
    for _ in 0..2 {
        let shared = limits();
        let result = execute_prepared(
            &prepared,
            "test",
            &json!({"nested":true}),
            "query",
            &mut |method, args| {
                assert_eq!(method, "get");
                assert_eq!(args, json!(["nested", null]));
                let child = execute_prepared(
                    &prepared,
                    "test",
                    &json!({"nested":false}),
                    "query",
                    &mut |method, args| {
                        assert_eq!(method, "get");
                        assert_eq!(args, json!(["leaf", null]));
                        Ok(json!(123))
                    },
                    shared.clone(),
                )?;
                Ok(child["value"].clone())
            },
            shared.clone(),
        )
        .unwrap();
        assert_eq!(result, json!({"ok":true,"value":[1,[1,123,1],1]}));
        shared.check().unwrap();
    }
    assert!(
        pool::stats(&prepared).created >= 2,
        "an active Store cannot service its recursive child"
    );
    assert!(pool::stats(&prepared).reused > 0);
}

#[test]
fn recycled_business_errors_reset_the_next_callback_and_other_bundles_stay_distinct() {
    let source = format!(
        "{STATIC_INIT_MARKER}let count=0;{}",
        bundle(
            "(_,args)=>{++count;if(args){count=90;throw Object.assign(Error('business'),{code:'DECLINED'})}return['first',count]}",
            false
        )
    );
    let first = prepare(&source, limits()).unwrap();
    let second = prepare(&bundle("()=>['second',1]", true), limits()).unwrap();
    let rejected = execute_prepared(
        &first,
        "test",
        &json!(true),
        "query",
        &mut |_, _| Ok(Value::Null),
        limits(),
    )
    .unwrap();
    assert_eq!(
        rejected,
        json!({"ok":false,"error":{"code":"DECLINED","message":"business"}})
    );
    for prepared in [&first, &first, &second, &second, &first, &first] {
        let result = execute_prepared(
            prepared,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        let label = if Arc::ptr_eq(prepared, &first) {
            "first"
        } else {
            "second"
        };
        assert_eq!(result, json!({"ok":true,"value":[label,1]}));
    }
    assert!(pool::stats(&first).reused + pool::stats(&second).reused >= 2);
}

#[test]
fn one_pool_slot_runs_sequential_callbacks_without_retaining_idle_guests() {
    const CHILD: &str = "FLOWER_TEST_RECYCLE_ONE_SLOT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "evaluator::wasm::tests::recycle::one_pool_slot_runs_sequential_callbacks_without_retaining_idle_guests",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("FLOWER_WASM_POOL_SLOTS", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let prepared = prepare(
        &format!(
            "{STATIC_INIT_MARKER}let count=0;{}",
            bundle("()=>++count", false)
        ),
        limits(),
    )
    .unwrap();
    for _ in 0..3 {
        let result = execute_prepared(
            &prepared,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(result, json!({"ok":true,"value":1}));
        assert_eq!(pool::stats(&prepared).idle, 0);
    }
    assert_eq!(pool::stats(&prepared).created, 3);
    assert_eq!(pool::stats(&prepared).reused, 0);
}

#[test]
fn insufficient_recycle_byte_budgets_execute_without_copying_or_tracking() {
    const CHILD: &str = "FLOWER_TEST_RECYCLE_BYTE_BUDGET_CHILD";
    if std::env::var_os(CHILD).is_none() {
        for budget in ["0", "1"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "evaluator::wasm::tests::recycle::insufficient_recycle_byte_budgets_execute_without_copying_or_tracking",
                    "--nocapture",
                ])
                .env(CHILD, budget)
                .env("FLOWER_WASM_RECYCLE_BYTES", budget)
                .env("FLOWER_WASM_POOL_SLOTS", "32")
                .env("FLOWER_WASM_DIRTY_PAGES", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "recycle byte budget {budget}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }
    let prepared = prepare(
        &format!(
            "{STATIC_INIT_MARKER}let count=0;const bytes=new Uint8Array([7,8]);{}",
            bundle("()=>[++count,bytes[0]++]", false)
        ),
        limits(),
    )
    .unwrap();
    for _ in 0..3 {
        let result = execute_prepared(
            &prepared,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(result, json!({"ok":true,"value":[1,7]}));
        assert_eq!(pool::stats(&prepared).idle, 0);
    }
    let stats = pool::stats(&prepared);
    assert_eq!(stats.created, 3);
    assert_eq!(stats.reused, 0);
    assert_eq!(stats.reset_bytes, 0);
    assert_eq!(stats.reset_total_bytes, 0);
    assert_eq!(stats.signal_faults, 0);
}

#[test]
fn recycled_entropy_permissions_follow_each_callback_kind_and_randomness_is_fresh() {
    let source = format!(
        r#"{STATIC_INIT_MARKER}
        const random=()=>Array.from(__flowerCrypto(0,32));
        var __flowerBundle={{default:{{definitions:{{
            write:{{name:'write',kind:'mutationMethod',compute:random}},
            read:{{name:'read',kind:'queryMethod',compute:random}},
            child:{{name:'child',kind:'derived',compute:random}}
        }},http:{{}}}}}};"#
    );
    let prepared = prepare(&source, limits()).unwrap();
    let mut samples = Vec::new();
    for (name, kind, allowed) in [
        ("write", "mutation", true),
        ("read", "query", false),
        ("write", "mutation", true),
        ("child", "derived", false),
    ] {
        let result = execute_prepared(
            &prepared,
            name,
            &Value::Null,
            kind,
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(result["ok"], allowed, "{result}");
        if allowed {
            samples.push(result["value"].clone());
        } else {
            assert!(result["error"]["message"]
                .as_str()
                .unwrap()
                .contains("only in mutations"));
        }
    }
    assert_ne!(samples[0], samples[1]);
    assert!(pool::stats(&prepared).reused >= 3);
}

#[test]
fn traps_and_memory_growth_discard_stores_and_limits_are_recharged() {
    let prepared = prepare(
        &bundle(
            r#"(_,args)=>{
        if(args==='loop'){try{for(;;){}}catch(e){}}
        if(args==='grow')return new Uint8Array(16*1024*1024).length;
        return 42;
    }"#,
            true,
        ),
        limits(),
    )
    .unwrap();
    let invoke = |args: &Value, shared: Arc<Limits>| {
        execute_prepared(
            &prepared,
            "test",
            args,
            "query",
            &mut |_, _| Ok(Value::Null),
            shared,
        )
    };
    assert_eq!(invoke(&Value::Null, limits()).unwrap()["value"], 42);
    let before = pool::stats(&prepared);
    let expired = Limits::new(Instant::now() + Duration::from_millis(30), MAX_MEMORY_BYTES);
    assert!(invoke(&json!("loop"), expired.clone()).is_err());
    assert!(expired.check().is_err());
    assert!(pool::stats(&prepared).discarded > before.discarded);
    let after_trap = pool::stats(&prepared);
    assert_eq!(invoke(&Value::Null, limits()).unwrap()["value"], 42);
    assert!(pool::stats(&prepared).created > after_trap.created);
    let before_growth = pool::stats(&prepared);
    assert_eq!(
        invoke(&json!("grow"), limits()).unwrap()["value"],
        16 * 1024 * 1024
    );
    assert!(pool::stats(&prepared).discarded > before_growth.discarded);
    assert_eq!(invoke(&Value::Null, limits()).unwrap()["value"], 42);
    // A parked guest must not bypass a later transaction's smaller allowance.
    let tiny = Limits::new(Instant::now() + Duration::from_secs(30), 1);
    assert!(invoke(&Value::Null, tiny.clone()).is_err());
    assert!(tiny.check().is_err());
    assert_eq!(invoke(&Value::Null, limits()).unwrap()["value"], 42);
}

#[test]
fn unwinding_callbacks_discard_active_stores_and_idle_guests_can_be_released() {
    let prepared = prepare(&bundle("()=>'released idle'", true), limits()).unwrap();
    let run = || {
        execute_prepared(
            &prepared,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
    };
    run().unwrap();
    assert_eq!(pool::stats(&prepared).idle, 1);
    pool::release(&prepared);
    assert_eq!(pool::stats(&prepared).idle, 0);
    let before = pool::stats(&prepared);
    run().unwrap();
    assert_eq!(pool::stats(&prepared).created, before.created + 1);
    // A host callback that unwinds mid-invocation drops its active Store; the
    // returned instance of an earlier call on the same image stays reusable.
    let calling = prepare(&bundle("ctx=>ctx.get('unwinding',null)", true), limits()).unwrap();
    let call = |host: &mut dyn FnMut(&str, Value) -> Result<Value>| {
        execute_prepared(&calling, "test", &Value::Null, "query", host, limits())
    };
    call(&mut |_, _| Ok(json!(1))).unwrap();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        call(&mut |_, _| panic!("test active host callback unwinding")).unwrap();
    }));
    assert!(unwind.is_err());
    let after = pool::stats(&calling);
    assert_eq!(
        after.idle, 0,
        "the unwinding invocation held the only idle guest"
    );
    assert_eq!(after.reused, 1);
    assert_eq!(
        call(&mut |_, _| Ok(json!(2))).unwrap(),
        json!({"ok":true,"value":2})
    );
    assert_eq!(pool::stats(&calling).created, after.created + 1);
}

#[test]
fn managed_authorization_is_resolved_again_after_store_reuse() {
    const CHILD: &str = "FLOWER_TEST_RECYCLE_MANAGED_CHILD";
    if std::env::var_os(CHILD).is_none() {
        use std::io::Write;
        let mut wrapping = tempfile::NamedTempFile::new().unwrap();
        wrapping.write_all(&[29; 32]).unwrap();
        let output=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","evaluator::wasm::tests::recycle::managed_authorization_is_resolved_again_after_store_reuse","--nocapture"])
            .env(CHILD,"1").env("FLOWER_KEYRING_FILE",wrapping.path())
            .env("FLOWER_KEY_CACHE_BYTES","16777216").env("FLOWER_KEY_CACHE_TTL_MS","0")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use crate::crypto::managed;
    let (catalog, _) = managed::prepare(
        None,
        &json!({"operation":"generate","name":"key","algorithm":"Ed25519"}),
    )
    .unwrap();
    let (catalog, _) = managed::prepare(
        Some(&catalog),
        &json!({"operation":"bind","name":"signer","key":"key","usages":["publicKey"]}),
    )
    .unwrap();
    let declaration =
        json!({"kind":"key","name":"signer","algorithm":"Ed25519","usages":["publicKey"]});
    let prepared = prepare(
        &bundle(
            r#"(_,key)=>{
        const request=JSON.stringify({operation:'key.publicKey',key});
        const first=__flowerCrypto(200,0,request,'','','');
        const second=__flowerCrypto(200,0,request,'','','');
        return[Array.from(first),Array.from(second)];
    }"#,
            true,
        ),
        limits(),
    )
    .unwrap();
    for allowed in [true, false, true] {
        let mut resolutions = 0;
        let value = execute_prepared(
            &prepared,
            "test",
            &declaration,
            "query",
            &mut |method, _| {
                assert_eq!(method, "managedKey");
                resolutions += 1;
                anyhow::ensure!(
                    allowed,
                    "KEY_FORBIDDEN: permission revoked for this invocation"
                );
                managed::resolve(&catalog, &declaration, "key.publicKey", None)
            },
            limits(),
        )
        .unwrap();
        assert_eq!(value["ok"], allowed, "{value}");
        assert_eq!(
            resolutions, 1,
            "authorization must cache within one invocation only"
        );
        if allowed {
            assert_eq!(value["value"][0], value["value"][1]);
        } else {
            assert!(value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("permission revoked"));
        }
    }
    assert!(pool::stats(&prepared).reused >= 2);
}
