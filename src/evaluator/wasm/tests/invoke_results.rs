use super::*;

#[test]
fn retained_invocation_text_survives_guest_drop_with_large_unicode_results() {
    for static_init in [false, true] {
        let code = bundle("(_,args)=>args", static_init);
        let prepared = prepare(&code, limits()).unwrap();
        for value in [
            Value::Null,
            json!({"text":"quote\"\\\n\0é🌸","nested":[true,false,1e21]}),
            json!("large\0é🌸".repeat(16384)),
        ] {
            let result = execute_prepared(
                &prepared,
                "test",
                &value,
                "query",
                &mut |_, _| Ok(Value::Null),
                limits(),
            )
            .unwrap();
            assert_eq!(result, json!({"ok":true,"value":value}));
        }
    }
}

#[test]
fn initialization_exceptions_fail_the_invocation_and_callback_exceptions_are_outcomes() {
    let error = run(
        &format!(
            "throw Error('initialization 🌸\\0tail');{}",
            bundle("()=>1", false)
        ),
        Value::Null,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("initialization 🌸\0tail"),
        "{error:#}"
    );
    let result = run(
        &bundle("()=>{throw Error('business 🌸\\0tail')}", true),
        Value::Null,
    )
    .unwrap();
    assert_eq!(
        result,
        json!({"ok":false,"error":{"code":"COMPUTE_ERROR","message":"business 🌸\0tail"}})
    );
}
