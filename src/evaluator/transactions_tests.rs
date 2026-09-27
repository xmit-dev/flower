use super::*;

fn deployed(body: &str) -> Records {
    let javascript = format!(
        r#"var __flowerBundle={{default:{{definitions:{{plan:{{name:'plan',kind:'transactionMethod',compute:(ctx,args)=>{{{body}}}}}}},http:{{transfer:{{name:'plan',kind:'transaction'}}}}}}}};"#
    );
    let result = evaluate(BTreeMap::new(), json!({"requestId":"deploy","bundle":{"hash":hash(javascript.as_bytes()),"javascript":javascript}})).unwrap();
    result.puts.into()
}

#[test]
fn cross_group_plan_is_a_private_pure_definition_with_an_explicit_alias() {
    let data = deployed(
        "return {calls:[{group:'a',method:'debit',args},{group:'b',method:'credit',args}],value:args.amount};",
    );
    assert_eq!(data["httpMethods"]["transfer"]["kind"], "transaction");
    let result = invoke_at(
        data,
        json!({"name":"plan","args":{"amount":7},"requestId":"transfer-1"}),
        "transaction",
        100,
    )
    .unwrap();
    assert_eq!(result.value["calls"].as_array().unwrap().len(), 2);
    assert_eq!(result.value["value"], 7);
    assert!(result.puts.is_empty());
    assert!(result.deletes.is_empty());
}

#[test]
fn transaction_planning_cannot_read_or_write_database_state_even_if_caught() {
    for body in [
        "ctx.now();return {calls:[]};",
        "ctx.get({kind:'collection',name:'account'},'a');return {calls:[]};",
        "try {ctx.set({kind:'collection',name:'account'},'a',1);}catch(e){} return {calls:[]};",
    ] {
        let result = invoke_at(
            deployed(body),
            json!({"name":"plan","requestId":"plan-1"}),
            "transaction",
            100,
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TRANSACTION_PLAN_ONLY")
        );
    }
}
