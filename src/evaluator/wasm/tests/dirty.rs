//! Protection failures terminate a subprocess rather than the full test runner.
use super::super::{dirty as tracking, recycle as pool};
use super::*;

fn child(test: &str) -> bool {
    child_in(test, "1")
}

/// Run `test` alone in a subprocess with this FLOWER_WASM_DIRTY_PAGES mode.
fn child_in(test: &str, mode: &str) -> bool {
    const CHILD: &str = "FLOWER_TEST_DIRTY_PAGES_CHILD";
    if std::env::var(CHILD).is_ok_and(|name| name == test) {
        // Compile the runtime first: a loaded debug build can otherwise spend
        // a test's whole evaluation deadline preparing it.
        warmup().unwrap();
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("evaluator::wasm::tests::dirty::{test}"),
            "--nocapture",
        ])
        .env(CHILD, test)
        .env("FLOWER_WASM_DIRTY_PAGES", mode)
        .env("FLOWER_WASM_RECYCLE", "1")
        .env("FLOWER_WASM_POOL_SLOTS", "32")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "dirty-page child {test}: {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

struct Unprotect(Arc<tracking::Tracker>);
impl Drop for Unprotect {
    fn drop(&mut self) {
        self.0.unprotect().unwrap();
    }
}

/// A minimal module whose whole memory is directly tracked. Fields drop in
/// order, so protection ends before the Store goes away.
struct Direct {
    _unprotect: Unprotect,
    tracker: Arc<tracking::Tracker>,
    store: wasmtime::Store<Host>,
    memory: wasmtime::Memory,
    store_i64: wasmtime::TypedFunc<(i32, i64), ()>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    load_i64: wasmtime::TypedFunc<i32, i64>,
    pristine: Vec<u8>,
    page: usize,
}

impl Direct {
    fn new(wasm_pages: u64) -> Self {
        use wasm_encoder::{
            CodeSection, ExportKind, ExportSection, Function, FunctionSection, Instruction, MemArg,
            MemorySection, MemoryType, TypeSection, ValType,
        };
        // Creating both the production Engine and a separate test Engine in one
        // process also exercises Wasmtime's process-wide macOS signal-mode contract.
        let _runtime = super::super::cache::runtime().unwrap();
        let mut config = wasmtime::Config::new();
        config.macos_use_mach_ports(false);
        let engine = wasmtime::Engine::new(&config).unwrap();
        let mut wasm = wasm_encoder::Module::new();
        let mut types = TypeSection::new();
        types.ty().function([ValType::I32, ValType::I64], []);
        types.ty().function([ValType::I32], [ValType::I64]);
        wasm.section(&types);
        let mut functions = FunctionSection::new();
        functions.function(0);
        functions.function(1);
        wasm.section(&functions);
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: wasm_pages,
            maximum: Some(wasm_pages),
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        wasm.section(&memories);
        let mut exports = ExportSection::new();
        exports.export("memory", ExportKind::Memory, 0);
        exports.export("write", ExportKind::Func, 0);
        exports.export("read", ExportKind::Func, 1);
        wasm.section(&exports);
        let access = MemArg {
            offset: 0,
            align: 0,
            memory_index: 0,
        };
        let mut write = Function::new([]);
        write.instruction(&Instruction::LocalGet(0));
        write.instruction(&Instruction::LocalGet(1));
        write.instruction(&Instruction::I64Store(access));
        write.instruction(&Instruction::End);
        let mut read = Function::new([]);
        read.instruction(&Instruction::LocalGet(0));
        read.instruction(&Instruction::I64Load(access));
        read.instruction(&Instruction::End);
        let mut code = CodeSection::new();
        code.function(&write);
        code.function(&read);
        wasm.section(&code);
        let module = wasmtime::Module::new(&engine, wasm.finish()).unwrap();
        let mut store = wasmtime::Store::new(&engine, Host::new(limits(), None));
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        let store_i64 = instance.get_typed_func(&mut store, "write").unwrap();
        let load_i64 = instance.get_typed_func(&mut store, "read").unwrap();
        store_i64.call(&mut store, (0, 0)).unwrap();
        memory.data_mut(&mut store).fill(0x5a);
        let pristine = memory.data(&store).to_vec();
        let tracker = tracking::install(&mut store, memory, Default::default())
            .unwrap()
            .expect("supported OS page geometry");
        let unprotect = Unprotect(tracker.clone());
        store.data_mut().dirty = Some(tracker.clone());
        tracker.protect().unwrap();
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        assert!(page > 0 && memory.data_size(&store) >= 8 * page);
        Self {
            _unprotect: unprotect,
            tracker,
            store,
            memory,
            store_i64,
            load_i64,
            pristine,
            page,
        }
    }

    fn pages(&self) -> usize {
        self.memory.data_size(&self.store) / self.page
    }

    fn write(&mut self, offset: usize, value: i64) {
        self.store_i64
            .call(&mut self.store, (offset as i32, value))
            .unwrap();
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn read(&mut self, offset: usize) -> i64 {
        self.load_i64.call(&mut self.store, offset as i32).unwrap()
    }

    /// Reset, check every byte, and return the bytes copied.
    fn restore(&mut self) -> usize {
        let restored = self
            .tracker
            .restore(self.memory.data_mut(&mut self.store), &self.pristine)
            .unwrap();
        assert!(restored.reusable);
        assert_eq!(
            self.memory.data(&self.store),
            self.pristine,
            "restore compares every memory byte"
        );
        restored.copied
    }
}

#[test]
fn dirty_pages_restore_guest_writes_and_track_large_recursive_host_writes() {
    if !child("dirty_pages_restore_guest_writes_and_track_large_recursive_host_writes") {
        return;
    }
    // Retain spare heap capacity in the snapshot so large input/result buffers
    // exercise reset of one mapping instead of merely forcing memory growth.
    let source = format!(
        "{STATIC_INIT_MARKER}let count=0;const bytes=new Uint8Array(512*1024);bytes.fill(31);let slack=new Uint8Array(1024*1024);slack=null;{}",
        bundle(
            r#"(ctx,args)=>{
                const pristine=bytes.every(value=>value===31);
                const index=args.page*16384;
                bytes[index]=args.mark;
                const host=ctx.get(args.nested?'nested':'leaf',{text:args.text});
                const message=new Uint8Array(args.text.length);message.fill(args.mark);
                const nonce=new Uint8Array(24),key=new Uint8Array(32);
                const sealed=__flowerCrypto(1,0,message,nonce,key);
                const opened=__flowerCrypto(2,0,sealed,nonce,key);
                return{pristine,count:++count,mark:bytes[index],host,
                    crypto:opened.length===message.length&&opened.every(value=>value===args.mark)};
            }"#,
            false,
        ),
    );
    let prepared = prepare(&source, limits()).unwrap();
    // Later calls touch previously untouched pages; once a page becomes writable
    // it must remain part of every subsequent reset, even if a call skips it.
    for (index, (page, text)) in [
        (0, String::new()),
        (3, "a".repeat(16_384)),
        (17, "b".repeat(49_151)),
        (9, "c".repeat(65_537)),
        (3, "🌺\0".repeat(4096)),
        (0, String::new()),
    ]
    .into_iter()
    .enumerate()
    {
        let mark = index + 1;
        let shared = limits();
        let args = json!({"page":page,"mark":mark,"text":text,"nested":true});
        let result = execute_prepared(
            &prepared,
            "test",
            &args,
            "query",
            &mut |name, args| {
                assert_eq!(name, "get");
                assert_eq!(args, json!(["nested",{"text":text}]));
                // This child's input writes happen while its parent is inside a
                // host call. The child must mark its own protected mapping.
                let child_text = "child".repeat(8193);
                let child = execute_prepared(
                    &prepared,
                    "test",
                    &json!({"page":page,"mark":99,"text":child_text,"nested":false}),
                    "query",
                    &mut |name, args| {
                        assert_eq!(name, "get");
                        assert_eq!(args, json!(["leaf",{"text":child_text}]));
                        Ok(json!({"text":child_text}))
                    },
                    shared.clone(),
                )?;
                assert_eq!(child["ok"], true, "{child}");
                assert_eq!(child["value"]["pristine"], true, "{child}");
                assert_eq!(child["value"]["count"], 1, "{child}");
                assert_eq!(child["value"]["mark"], 99, "{child}");
                assert_eq!(child["value"]["crypto"], true, "{child}");
                assert_eq!(child["value"]["host"], json!({"text":child_text}));
                Ok(json!({"text":text}))
            },
            shared.clone(),
        )
        .unwrap();
        assert_eq!(
            result,
            json!({"ok":true,"value":{
                "pristine":true,"count":1,"mark":mark,"host":{"text":text},"crypto":true
            }}),
            "invocation {index}"
        );
        shared.check().unwrap();
    }
    assert!(
        pool::stats(&prepared).reused >= 2,
        "must exercise protected Store reuse"
    );
    let stats = pool::stats(&prepared);
    assert!(
        stats.signal_faults > 0,
        "must handle actual guest write faults"
    );
    assert!(
        stats.reset_bytes < stats.reset_total_bytes,
        "clean pages must avoid full-image copies: {stats:?}"
    );
}

#[test]
fn dirty_page_failures_release_protection_and_do_not_reuse_poisoned_guests() {
    if !child("dirty_page_failures_release_protection_and_do_not_reuse_poisoned_guests") {
        return;
    }
    let source = format!(
        "{STATIC_INIT_MARKER}let count=0;{}",
        bundle(
            r#"(ctx,args)=>{
                ++count;
                if(args==='business')throw Object.assign(Error('declined'),{code:'DECLINED'});
                if(args==='loop'){try{for(;;){}}catch(e){}}
                if(args==='grow')return new Uint8Array(16*1024*1024).length;
                if(args==='host')ctx.get('panic',null);
                return count;
            }"#,
            false,
        ),
    );
    let prepared = prepare(&source, limits()).unwrap();
    let invoke = |argument: &Value, shared: Arc<Limits>| {
        execute_prepared(
            &prepared,
            "test",
            argument,
            "query",
            &mut |_, _| panic!("intentional active host panic"),
            shared,
        )
    };
    let check_fresh = || {
        assert_eq!(
            invoke(&Value::Null, limits()).unwrap(),
            json!({"ok":true,"value":1})
        );
    };
    {
        check_fresh();
        assert_eq!(
            invoke(&json!("business"), limits()).unwrap()["error"]["code"],
            "DECLINED"
        );
        check_fresh();
        let before = pool::stats(&prepared);
        let short = Limits::new(Instant::now() + Duration::from_millis(30), MAX_MEMORY_BYTES);
        assert!(invoke(&json!("loop"), short.clone()).is_err());
        assert!(short.check().is_err());
        assert!(pool::stats(&prepared).discarded > before.discarded);
        check_fresh();
        let before = pool::stats(&prepared);
        assert_eq!(
            invoke(&json!("grow"), limits()).unwrap()["value"],
            16 * 1024 * 1024
        );
        assert!(pool::stats(&prepared).discarded > before.discarded);
        check_fresh();
        let tiny = Limits::new(Instant::now() + Duration::from_secs(30), 1);
        assert!(invoke(&Value::Null, tiny.clone()).is_err());
        assert!(tiny.check().is_err());
        check_fresh();
    }
    pool::release(&prepared);
    assert_eq!(pool::stats(&prepared).idle, 0);
    // The panicking invocation takes the only idle guest and must discard it.
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        check_fresh();
        invoke(&json!("host"), limits()).unwrap();
    }));
    assert!(unwind.is_err());
    assert_eq!(pool::stats(&prepared).idle, 0);
    check_fresh();
}

#[test]
fn dirty_page_faults_cover_cross_page_writes_and_leave_oob_traps_intact() {
    if !child_in(
        "dirty_page_faults_cover_cross_page_writes_and_leave_oob_traps_intact",
        "signal",
    ) {
        return;
    }
    let mut direct = Direct::new(8);
    assert!(!direct.tracker.kernel_tracked());
    let page = direct.page;
    #[cfg(target_os = "macos")]
    let errno = unsafe { libc::__error() };
    #[cfg(target_os = "linux")]
    let errno = unsafe { libc::__errno_location() };
    unsafe {
        *errno = libc::EDOM;
    }
    direct.write(0, 11);
    assert_eq!(
        unsafe { *errno },
        libc::EDOM,
        "signal handling must preserve errno"
    );
    assert_eq!(direct.tracker.faults(), 1);
    // The first half is already writable. The fault must identify and unlock
    // the second page, allowing the original unaligned store to complete.
    direct.write(page - 4, 0x1234_5678_1357_2468);
    assert_eq!(direct.tracker.faults(), 2);
    let last = direct.memory.data_size(&direct.store) - 8;
    direct.write(last, 19);
    assert_eq!(direct.tracker.faults(), 3);
    assert_eq!(direct.restore(), 3 * page);
    assert_eq!(
        direct.restore(),
        3 * page,
        "ever-dirty pages remain in later resets even without another fault"
    );
    // A host payload with a terminator crosses two still-clean pages outside
    // any Wasm activation. Missing explicit marking would crash this subprocess.
    let start = 3 * page - 1;
    tracking::prepare_write(&mut direct.store, start, 3).unwrap();
    direct.memory.data_mut(&mut direct.store)[start..start + 3].copy_from_slice(b"a\0\0");
    assert_eq!(
        direct.tracker.faults(),
        3,
        "host writes need no guest fault handler"
    );
    assert_eq!(direct.restore(), 5 * page);
    unsafe {
        *errno = libc::EDOM;
    }
    assert!(tracking::prepare_write(&mut direct.store, usize::MAX, 2).is_err());
    assert_eq!(unsafe { *errno }, libc::EDOM);
    let out_of_bounds = direct.memory.data_size(&direct.store) as i32;
    let error = direct
        .store_i64
        .call(&mut direct.store, (out_of_bounds, 23))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::MemoryOutOfBounds)
    );
    assert_eq!(
        direct.tracker.faults(),
        3,
        "an OOB fault must never be swallowed"
    );
    assert_eq!(direct.memory.data(&direct.store), direct.pristine);
}

#[test]
fn signal_tracking_trims_unusual_growth_and_periodically_relearns() {
    if !child_in(
        "signal_tracking_trims_unusual_growth_and_periodically_relearns",
        "signal",
    ) {
        return;
    }
    let mut direct = Direct::new(16);
    let (page, pages) = (direct.page, direct.pages());
    direct.write(0, 1);
    assert_eq!(
        direct.restore(),
        page,
        "the first footprint sets the baseline"
    );
    // One unusual callback writes every page. Its reset is complete, then the
    // hot set is far beyond twice the baseline, so those pages are protected
    // again instead of being copied by every later reset.
    for index in 0..pages {
        direct.write(index * page, 2);
    }
    assert_eq!(direct.restore(), pages * page);
    assert_eq!(direct.tracker.hot_pages(), 0);
    assert_eq!(direct.restore(), 0);
    let faults = direct.tracker.faults();
    direct.write(0, 3);
    assert_eq!(
        direct.tracker.faults(),
        faults + 1,
        "a trimmed page faults again"
    );
    assert_eq!(direct.restore(), page);
    assert_eq!(direct.restore(), page, "and then stays hot");
    // Growth that comes back right after a trim is the workload: it is
    // trimmed once, then kept instead of faulting on every callback.
    for round in 0..3 {
        for index in 0..pages {
            direct.write(index * page, 5 + round);
        }
        assert_eq!(direct.restore(), pages * page);
    }
    assert_eq!(direct.tracker.hot_pages(), pages);
    let faults = direct.tracker.faults();
    for index in 0..pages {
        direct.write(index * page, 9);
    }
    assert_eq!(direct.tracker.faults(), faults);
    assert_eq!(direct.restore(), pages * page);
    // Periodic relearning protects every page again.
    let mut resets = 0;
    while direct.tracker.hot_pages() != 0 {
        assert_eq!(direct.restore(), pages * page);
        resets += 1;
        assert!(resets <= 1024, "relearning must protect hot pages again");
    }
    assert_eq!(direct.restore(), 0);
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_tracking_copies_exactly_the_pages_written_since_the_previous_reset() {
    if !child("kernel_tracking_copies_exactly_the_pages_written_since_the_previous_reset") {
        return;
    }
    let mut direct = Direct::new(8);
    if !direct.tracker.kernel_tracked() {
        eprintln!("userfaultfd write protection is unavailable; signal tests cover this host");
        return;
    }
    let page = direct.page;
    // Reads never count as writes, even of pages not yet populated.
    for index in 0..direct.pages() {
        assert_eq!(direct.read(index * page), 0x5a5a_5a5a_5a5a_5a5a);
    }
    assert_eq!(direct.restore(), 0);
    direct.write(0, 1);
    // An unaligned store crossing pages 2 and 3.
    direct.write(3 * page - 4, 0x1234_5678_1357_2468);
    // The kernel also records host writes, without prepare_write...
    direct.memory.data_mut(&mut direct.store)[5 * page] = 7;
    // ...and writes made by system calls into guest memory.
    unsafe {
        let mut pipe = [0; 2];
        assert_eq!(libc::pipe(pipe.as_mut_ptr()), 0);
        assert_eq!(libc::write(pipe[1], b"xyz".as_ptr().cast(), 3), 3);
        let target = direct.memory.data_mut(&mut direct.store)[6 * page..].as_mut_ptr();
        assert_eq!(libc::read(pipe[0], target.cast(), 3), 3);
        libc::close(pipe[0]);
        libc::close(pipe[1]);
    }
    assert_eq!(direct.restore(), 5 * page);
    assert_eq!(direct.restore(), 0, "each reset protects its pages again");
    // A page written by two resets within the promotion window stays hot and
    // is copied by every reset without further faults.
    direct.write(0, 2);
    assert_eq!(direct.restore(), page);
    assert_eq!(direct.tracker.hot_pages(), 1);
    let faults = direct.tracker.faults();
    assert_eq!(direct.restore(), page);
    direct.write(0, 3);
    assert_eq!(direct.restore(), page);
    assert_eq!(direct.tracker.faults(), faults);
    // Outside that window, one unusual callback writing every page does not
    // make later resets copy more.
    for _ in 0..4 {
        direct.restore();
    }
    for index in 0..direct.pages() {
        direct.write(index * page, 4);
    }
    assert_eq!(direct.restore(), direct.pages() * page);
    assert_eq!(direct.restore(), page);
    let out_of_bounds = direct.memory.data_size(&direct.store) as i32;
    let error = direct
        .store_i64
        .call(&mut direct.store, (out_of_bounds, 23))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::MemoryOutOfBounds)
    );
    assert_eq!(direct.restore(), page);
}

#[test]
fn fresh_stores_start_with_the_learned_seed_and_skip_first_write_faults() {
    if !child("fresh_stores_start_with_the_learned_seed_and_skip_first_write_faults") {
        return;
    }
    let source = format!(
        "{STATIC_INIT_MARKER}let count=0;const bytes=new Uint8Array(256*1024);{}",
        bundle(
            "(_,args)=>{for(let i=0;i<bytes.length;i+=4096)bytes[i]=args;return ++count}",
            false,
        ),
    );
    let prepared = prepare(&source, limits()).unwrap();
    let mut faults = Vec::new();
    let mut copied = Vec::new();
    for _ in 0..3 {
        let before = pool::stats(&prepared);
        let result = execute_prepared(
            &prepared,
            "test",
            &json!(7),
            "query",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(result, json!({"ok":true,"value":1}));
        let after = pool::stats(&prepared);
        faults.push(after.signal_faults - before.signal_faults);
        copied.push(after.reset_bytes - before.reset_bytes);
        // The next callback must start in a new Store.
        pool::release(&prepared);
    }
    assert_eq!(pool::stats(&prepared).created, 3);
    // Two unseeded footprints teach the image its seed; the third Store starts
    // with those pages hot and copies exactly what the first two did.
    assert!(faults[0] > 0 && faults[1] > 0, "{faults:?}");
    assert_eq!(faults[2], 0, "{faults:?}");
    assert_eq!(copied[2], copied[0], "{copied:?}");
}
