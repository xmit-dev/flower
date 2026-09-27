//! Write Flower applications as WebAssembly guest modules in Rust.
//!
//! A module implements [GUEST_ABI.md](../../GUEST_ABI.md) directly: Rust owns
//! the reactive graph, storage and replication, and the guest runs one named
//! callback per invocation from a pristine image. This crate mirrors the
//! TypeScript SDK's semantics (`sdk/`): keyed collections, schemas, triggers,
//! materialization, maintenance tasks, [`scheduler`] timers and [`queue`]
//! leases produce the same records and host calls as their TypeScript
//! counterparts, so either guest can serve the same database.
//!
//! Build for `wasm32-unknown-unknown` as a `cdylib`, list the application in
//! a static [`App`], and call [`export!`] once.
#![cfg_attr(not(test), no_std)]
extern crate alloc;

pub mod abi;
mod app;
mod context;
mod failure;
pub mod json;
pub mod queue;
pub mod scheduler;
pub mod schema;
mod value;
pub mod wire;

pub use app::{
    Aggregate, App, Callback, Component, Compute, Consistency, Definition, Derived, Materialize,
    Method, Task, TaskFailure, Trigger, plain,
};
pub use context::{
    ANONYMOUS_SUBJECT, Change, Collection, Ctx, Index, IndexDef, Kind, Page, Query, Range,
    RangeQuery, Row, host,
};
pub use failure::{Failure, Result, fail, fail_with, type_error};
pub use value::{Map, NULL, Value};

#[doc(hidden)]
pub use alloc::vec as __vec;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
