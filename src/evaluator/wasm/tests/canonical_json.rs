use super::*;

#[test]
fn native_canonical_json_preserves_primitive_spelling_in_fresh_and_cow_guests() {
    for static_init in [false, true] {
        for (compute, expected) in [
            (
                r#"()=>__flowerCanonicalJson(["tenant","store",-0,null,true,"🌸\ud800"])"#,
                r#"["tenant","store",0,null,true,"🌸\ud800"]"#,
            ),
            (
                "()=>__flowerCanonicalJson([1e-7,1e21,Number.MIN_VALUE])",
                "[1e-7,1e+21,5e-324]",
            ),
            (
                "()=>{Array.prototype.toJSON=()=>{throw Error('must not run')};return __flowerCanonicalJson(['a','b'])}",
                r#"["a","b"]"#,
            ),
            (
                "()=>{const array=['a','b'];Object.setPrototypeOf(array,new Proxy({},{get(){throw Error('prototype accessed')}}));return __flowerCanonicalJson(array)}",
                r#"["a","b"]"#,
            ),
        ] {
            let code = bundle(compute, static_init);
            for _ in 0..2 {
                let result = run(&code, Value::Null).unwrap();
                assert_eq!(result["ok"], true, "{result}");
                assert_eq!(result["value"], expected, "{compute}");
            }
        }
    }
}

#[test]
fn native_canonical_fallback_does_not_invoke_traps_or_modified_intrinsics() {
    for compute in [
        "()=>{let calls=0;const array=new Proxy(['a'],{get(){calls++;throw Error('get')},getPrototypeOf(){calls++;throw Error('prototype')},ownKeys(){calls++;throw Error('keys')}});return __flowerCanonicalJson(array)===undefined&&calls===0}",
        "()=>{let calls=0;const array=['a'];Object.defineProperty(array,'0',{get(){calls++;throw Error('getter')}});return __flowerCanonicalJson(array)===undefined&&calls===0}",
        "()=>{let calls=0;const old=JSON.stringify;JSON.stringify=()=>{calls++;return 'wrong'};const result=__flowerCanonicalJson(['a'])===undefined&&calls===0;JSON.stringify=old;return result}",
        "()=>{let calls=0;RegExp.prototype.exec=()=>{calls++;throw Error('exec')};return __flowerCanonicalJson(['a'])===undefined&&calls===0}",
        "()=>{const array=['a'];array[Symbol('hidden')]=1;return __flowerCanonicalJson(array)===undefined}",
        "()=>__flowerCanonicalJson([undefined])===undefined&&__flowerCanonicalJson([,])===undefined&&__flowerCanonicalJson([{y:[{}],x:1}])==='[{\"x\":1,\"y\":[{}]}]'",
        "()=>{let calls=0;const object={a:1};Object.defineProperty(object,'b',{enumerable:true,get(){calls++;throw Error('getter')}});return __flowerCanonicalJson({nested:object})===undefined&&calls===0}",
        "()=>{let calls=0;const old=Array.prototype.sort;Array.prototype.sort=function(){calls++;return old.call(this)};const result=__flowerCanonicalJson({a:1})===undefined&&__flowerCanonicalJson(['a'])==='[\"a\"]'&&calls===0;Array.prototype.sort=old;return result}",
        "()=>{const d=Object.getOwnPropertyDescriptor(globalThis,'__flowerCanonicalJson');return !d.writable&&!d.configurable&&!d.enumerable}",
    ] {
        let result = run(&bundle(compute, false), Value::Null).unwrap();
        assert_eq!(result["value"], true, "{compute}: {result}");
    }
}
