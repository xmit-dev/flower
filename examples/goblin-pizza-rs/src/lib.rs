//! Goblin Pizza as a Rust guest module: examples/goblin-pizza-ts/goblin-pizza.ts, callback
//! for callback. It declares the same manifest, writes the same records and
//! makes the same host calls, so the benchmark and its audit run unchanged
//! against either guest. Money is an integer number of copper coins.
#![cfg_attr(target_arch = "wasm32", no_std)]
extern crate alloc;

use alloc::{format, string::String, vec::Vec};
use flower_sdk::{
    Aggregate, App, Collection, Consistency, Ctx, Definition, Derived, IndexDef, Map, Materialize,
    Method, Result, Value, array, fail, json, object,
    queue::{Enqueue, Queue},
    scheduler::Scheduler,
    schema::{Pattern, Schema, v},
};

pub const UNIT_PRICE: f64 = 7.0;
pub const MAX_LEASE_MS: f64 = 60_000.0;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

fn shop_key(shop: &Value) -> String {
    json::canonical(shop)
}

fn order_key(shop: &Value, id: &Value) -> String {
    let mut parts = shop.as_array().cloned().unwrap_or_default();
    parts.push(id.clone());
    json::canonical(&Value::Array(parts))
}

fn order_id(shop: &Value, id: &Value) -> Value {
    let mut parts = shop.as_array().cloned().unwrap_or_default();
    parts.push(id.clone());
    Value::Array(parts)
}

/// `/^[A-Za-z0-9_-]{1,96}$/`
fn identifier(text: &str) -> bool {
    (1..=96).contains(&text.len())
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

const IDENTIFIER: Schema = v::string().pattern(Pattern {
    source: "/^[A-Za-z0-9_-]{1,96}$/",
    test: identifier,
});
const SHOP_REF: Schema = v::tuple(&[IDENTIFIER, IDENTIFIER]);
const ORDER_REF: Schema = v::tuple(&[IDENTIFIER, IDENTIFIER, IDENTIFIER]);
const HISTORY: Schema = v::object(&[
    v::field("database", v::string()),
    v::field("incarnation", v::string()),
]);

static CONFIGURATION: Collection = Collection::new("pizza.config");
static TENANTS: Collection = Collection::new("pizza.tenants");
static SHOPS: Collection = Collection::new("pizza.shops").key(&SHOP_REF);
static ARCHIVED: Collection = Collection::new("pizza.archived").key(&SHOP_REF);
static ORDERS: Collection = Collection::new("pizza.orders")
    .key(&ORDER_REF)
    .indexes(&[IndexDef {
        name: "byShop",
        fields: &["shop"],
    }]);
// Each tenant's drones claim from their own scope of one indexed queue.
static DELIVERIES: Queue = Queue::new("pizza.deliveries")
    .lease(MAX_LEASE_MS, MAX_LEASE_MS)
    .retry(None);

fn number(value: &Value, key: &str) -> f64 {
    value.number(key).unwrap_or(f64::NAN)
}

fn config(ctx: &mut Ctx) -> Result<Value> {
    let settings = ctx.get(&CONFIGURATION, &Value::from("world"))?;
    if settings.is_null() {
        return fail("NOT_INITIALIZED", "The goblin kitchens are not open yet");
    }
    Ok(settings)
}

fn tenant_config(ctx: &mut Ctx, tenant: &str) -> Result<Value> {
    let settings = ctx.get(&TENANTS, &Value::from(tenant))?;
    if settings.is_null() {
        return fail("TENANT_NOT_FOUND", format!("No tenant named {tenant}"));
    }
    Ok(settings)
}

fn shop_record(ctx: &mut Ctx, id: &Value) -> Result<Value> {
    let shop = ctx.get(&SHOPS, id)?;
    if shop.is_null() {
        return fail(
            "SHOP_NOT_FOUND",
            format!("No goblin kitchen named {}", shop_key(id)),
        );
    }
    Ok(shop)
}

// Rust keeps a durable equality index and feeds only changed orders into these
// reversible reducers. A tip does not touch order totals; an order change costs
// one remove/add pair, regardless of how many pizzas the kitchen has sold.
fn adjust_orders(total: Value, order: &Value, direction: f64) -> Result<Value> {
    let status = order.text("status");
    let is = |state: &str| if status == Some(state) { 1.0 } else { 0.0 };
    let quantity = number(order, "quantity");
    Ok(object! {
        "orders" => number(&total, "orders") + direction,
        "baking" => number(&total, "baking") + direction * is("baking"),
        "ready" => number(&total, "ready") + direction * is("ready"),
        "delivered" => number(&total, "delivered") + direction * is("delivered"),
        "orderedQuantity" => number(&total, "orderedQuantity") + direction * quantity,
        "deliveredQuantity" => number(&total, "deliveredQuantity") + direction * if status == Some("delivered") { quantity } else { 0.0 },
    })
}

static ORDER_STATS_REDUCER: Aggregate = Aggregate {
    source: &ORDERS,
    index: "byShop",
    initial: |_| {
        Ok(
            object! {"orders" => 0, "baking" => 0, "ready" => 0, "delivered" => 0, "orderedQuantity" => 0, "deliveredQuantity" => 0},
        )
    },
    add: |total, order, _, _| adjust_orders(total, order, 1.0),
    remove: |total, order, _, _| adjust_orders(total, order, -1.0),
};
static ORDER_STATS: Derived = Derived::aggregate("pizza.orderStats", &ORDER_STATS_REDUCER);

// One maintained summary per kitchen, created and removed with its shop row.
static SHOP_SUMMARY: Derived = Derived::new("pizza.shopSummary", |ctx, id| {
    let shop = shop_record(ctx, &id)?;
    let stats = ctx.get_derived(&ORDER_STATS, &id)?;
    Ok(shop.with(&stats))
})
.materialize(Materialize::Each(&SHOPS));

fn score(shop: &Value) -> f64 {
    number(shop, "revenue") + number(shop, "tips")
}

fn by_key(a: &Value, b: &Value) -> core::cmp::Ordering {
    json::compare(
        a.text("key").unwrap_or_default(),
        b.text("key").unwrap_or_default(),
    )
}

// Rankings are an observational projection of the snapshot's durable summaries.
// Computing them on read avoids sorting and replicating a whole tenant ranking
// for every tip. Watching the dashboard still sees a coherent ranking and totals.
static LEADERBOARD: Derived = Derived::new("pizza.leaderboard", |ctx, tenant| {
    let settings = tenant_config(ctx, tenant.as_str().unwrap_or_default())?;
    let mut summaries = Vec::new();
    for id in settings.get("shopIds").as_array().into_iter().flatten() {
        summaries.push(ctx.get_derived(&SHOP_SUMMARY, id)?);
    }
    summaries.sort_by(|a, b| {
        let difference = score(b) - score(a);
        if difference != 0.0 && !difference.is_nan() {
            return if difference < 0.0 {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Greater
            };
        }
        by_key(a, b)
    });
    Ok(Value::Array(summaries))
});

// A private callback represents the oven bell. The status transition and queue
// insertion commit together; an interrupted attempt cannot publish half a pizza.
const FINISH_BAKING_ARGS: Schema =
    v::object(&[v::field("id", IDENTIFIER), v::field("shop", SHOP_REF)]);
static FINISH_BAKING: Method = Method::new("internal.pizza.finishBaking", |ctx, args| {
    let (id, shop) = (args.get("id"), args.get("shop"));
    let key = order_id(shop, id);
    let order = ctx.get(&ORDERS, &key)?;
    if order.is_null() {
        return fail("ORDER_NOT_FOUND", "The oven lost its order");
    }
    if order.text("status") != Some("baking") {
        return Ok(Value::Null);
    }
    let ready_at = ctx.now()?;
    let mut ready = order.clone();
    ready.set("status", "ready");
    ready.set("readyAt", ready_at);
    ctx.set(&ORDERS, &key, ready)?;
    let payload = object! {"orderId" => order.get("id"), "shop" => order.get("shop"), "quantity" => order.get("quantity")};
    DELIVERIES.scope(shop.at(0).as_str().unwrap_or_default()).enqueue(
        ctx,
        order.text("key").unwrap_or_default(),
        payload,
        Enqueue::default(),
    )?;
    Ok(Value::Null)
})
.args(&FINISH_BAKING_ARGS);

static OVEN_HANDLERS: [(&str, &Method); 1] = [("bake", &FINISH_BAKING)];
static OVENS: Scheduler =
    Scheduler::new("pizza.ovens", &OVEN_HANDLERS).retries(3.0, 100.0, 1_000.0);

const SETUP_ARGS: Schema = v::object(&[
    v::field("tenants", v::array(&IDENTIFIER).min(1.0)),
    v::field("storesPerTenant", v::int().min(1.0)),
    v::field("stockPerShop", v::int().min(1.0).max(1_000_000.0)),
    v::field("bakeMs", v::int().min(0.0).max(60_000.0)),
    v::field("leaseMs", v::int().min(1.0).max(MAX_LEASE_MS)),
]);
static SETUP: Method = Method::new("internal.pizza.setup", |ctx, input| {
    let tenants: Vec<Value> = input.get("tenants").as_array().cloned().unwrap_or_default();
    let stores = number(&input, "storesPerTenant");
    if tenants.iter().enumerate().any(|(index, tenant)| tenants[..index].contains(tenant)) {
        return fail("INVALID_ARGUMENT", "Tenant IDs must be distinct");
    }
    if !flower_sdk::schema::is_safe_integer(tenants.len() as f64 * stores) {
        return fail("INVALID_ARGUMENT", "Too many stores");
    }
    let world = Value::from("world");
    if !ctx.get(&CONFIGURATION, &world)?.is_null() {
        return fail("ALREADY_INITIALIZED", "The kitchens are already open; use a fresh database for another run");
    }
    const NAMES: [&str; 6] =
        ["The Crispy Cauldron", "Mushroom Mayhem", "The Sizzling Slime", "Dough or Die", "The Goblin's Slice", "Dragon Breath Delivery"];
    let shop_ids: Vec<Value> = tenants
        .iter()
        .flat_map(|tenant| (0..stores as usize).map(move |index| array![tenant, format!("store-{index}")]))
        .collect();
    let initial_stock = input.get("stockPerShop").clone();
    let settings = object! {
        "tenantIds" => tenants.clone(), "shopIds" => shop_ids.clone(), "storesPerTenant" => input.get("storesPerTenant"),
        "initialStock" => &initial_stock, "bakeMs" => input.get("bakeMs"), "leaseMs" => input.get("leaseMs"), "unitPrice" => UNIT_PRICE,
    };
    ctx.set(&CONFIGURATION, &world, settings.clone())?;
    for tenant in &tenants {
        let owned: Vec<Value> = shop_ids.iter().filter(|id| id.at(0) == tenant).cloned().collect();
        let mut scoped = settings.clone();
        scoped.set("tenantIds", array![tenant]);
        scoped.set("shopIds", owned);
        ctx.set(&TENANTS, tenant, scoped)?;
    }
    for (index, id) in shop_ids.iter().enumerate() {
        let name = format!("{} · {}", NAMES[index % NAMES.len()], id.at(1).as_str().unwrap_or_default());
        let shop = object! {
            "id" => id, "key" => shop_key(id), "name" => name, "initialStock" => &initial_stock, "stock" => &initial_stock,
            "revenue" => 0, "tips" => 0,
        };
        ctx.set(&SHOPS, id, shop)?;
    }
    Ok(settings)
})
.args(&SETUP_ARGS);

const ORDER_ARGS: Schema = v::object(&[
    v::field("id", IDENTIFIER),
    v::field("shop", SHOP_REF),
    v::field("quantity", v::int().min(1.0).max(4.0)),
]);
static PLACE_ORDER: Method = Method::new("internal.pizza.order", |ctx, input| {
    let shop = shop_record(ctx, input.get("shop"))?;
    let settings = tenant_config(ctx, shop.get("id").at(0).as_str().unwrap_or_default())?;
    let key = order_id(shop.get("id"), input.get("id"));
    if !ctx.get(&ORDERS, &key)?.is_null() {
        return fail("ORDER_EXISTS", "That pizza order already exists in this store");
    }
    let quantity = number(&input, "quantity");
    if number(&shop, "stock") < quantity {
        return fail("OUT_OF_STOCK", "The goblins have run out of enchanted dough");
    }
    let created_at = ctx.now()?;
    let due_at = ctx.now()? + number(&settings, "bakeMs");
    let order = object! {
        "id" => input.get("id"), "key" => order_key(shop.get("id"), input.get("id")), "shop" => shop.get("id"),
        "quantity" => quantity, "status" => "baking", "createdAt" => created_at, "dueAt" => due_at,
        "readyAt" => Value::Null, "deliveredAt" => Value::Null,
    };
    let mut restocked = shop.clone();
    restocked.set("stock", number(&shop, "stock") - quantity);
    ctx.set(&SHOPS, shop.get("id"), restocked)?;
    ctx.set(&ORDERS, &key, order.clone())?;
    let timer = format!("bake:{}", order.text("key").unwrap_or_default());
    OVENS.after(ctx, &timer, number(&settings, "bakeMs"), "bake", object! {"id" => order.get("id"), "shop" => shop.get("id")})?;
    Ok(order)
})
.args(&ORDER_ARGS);

const CLAIM_ARGS: Schema = v::object(&[
    v::field("tenant", IDENTIFIER),
    v::field("owner", IDENTIFIER),
    v::optional("leaseMs", v::int().min(1.0)),
]);
static CLAIM_DELIVERY: Method = Method::new("internal.pizza.claim", |ctx, input| {
    let tenant = input.text("tenant").unwrap_or_default();
    let settings = tenant_config(ctx, tenant)?;
    let limit = number(&settings, "leaseMs");
    let lease_ms = input.number("leaseMs").unwrap_or(limit);
    if lease_ms > limit {
        return fail(
            "INVALID_ARGUMENT",
            format!("Leases last at most {} ms", json::number(limit)),
        );
    }
    DELIVERIES
        .scope(tenant)
        .claim(ctx, input.text("owner").unwrap_or_default(), Some(lease_ms))
})
.args(&CLAIM_ARGS);

const DELIVER_ARGS: Schema = v::object(&[
    v::field("tenant", IDENTIFIER),
    v::field("id", v::string().min(1.0)),
    v::field("owner", IDENTIFIER),
    v::field("token", v::int().min(1.0)),
    v::optional("history", HISTORY),
]);
static DELIVER_PIZZA: Method = Method::new("internal.pizza.deliver", |ctx, input| {
    let tenant = input.text("tenant").unwrap_or_default();
    let mut identity: Map = input.as_object().cloned().unwrap_or_default();
    identity.remove("tenant");
    let identity = Value::Object(identity);
    tenant_config(ctx, tenant)?;
    let delivered_at = ctx.now()?;
    // Validate the lease before accounting. An expired or replaced drone cannot
    // collect coins; a failed transaction also discards this staged completion.
    let job = DELIVERIES.scope(tenant).complete(
        ctx,
        &identity,
        object! {"deliveredAt" => delivered_at},
    )?;
    let (shop, id) = (
        job.get("payload").get("shop"),
        job.get("payload").get("orderId"),
    );
    if shop.at(0).as_str() != Some(tenant)
        || Some(order_key(shop, id).as_str()) != identity.text("id")
    {
        return fail("LEASE_LOST", "Delivery tenant and identity must match");
    }
    let key = order_id(shop, id);
    let order = ctx.get(&ORDERS, &key)?;
    if order.is_null() {
        return fail("ORDER_NOT_FOUND", "This drone has no pizza");
    }
    if order.text("status") != Some("ready") {
        return fail("ORDER_NOT_READY", "Only a ready pizza can be delivered");
    }
    let shop = shop_record(ctx, order.get("shop"))?;
    let mut delivered = order.clone();
    delivered.set("status", "delivered");
    delivered.set("deliveredAt", delivered_at);
    ctx.set(&ORDERS, &key, delivered.clone())?;
    let mut paid = shop.clone();
    paid.set(
        "revenue",
        number(&shop, "revenue") + number(&order, "quantity") * UNIT_PRICE,
    );
    ctx.set(&SHOPS, shop.get("id"), paid)?;
    Ok(delivered)
})
.args(&DELIVER_ARGS);

const TIP_ARGS: Schema = v::object(&[
    v::field("shop", SHOP_REF),
    v::field("amount", v::int().min(1.0).max(1_000_000.0)),
]);
static TIP_KITCHEN: Method = Method::new("internal.pizza.tip", |ctx, input| {
    let shop = shop_record(ctx, input.get("shop"))?;
    let tips = number(&shop, "tips") + number(&input, "amount");
    if tips > MAX_SAFE_INTEGER - number(&shop, "initialStock") * UNIT_PRICE {
        return fail("INVALID_ARGUMENT", "Total tips overflow");
    }
    let mut tipped = shop.clone();
    tipped.set("tips", tips);
    ctx.set(&SHOPS, shop.get("id"), tipped)?;
    Ok(object! {"shop" => shop.get("id"), "tips" => tips})
})
.args(&TIP_ARGS);

// A long-running kitchen clears delivered orders off the board so its rows,
// queue and dashboard stay the size of the work in flight. Their counts move
// to one tally per kitchen in the same transaction; revenue stays on the shop.
// Summaries and pizza.world then cover live orders only; the dashboard adds
// the tallies back. The benchmark never archives, so it audits every order.
const ARCHIVE_ARGS: Schema = v::object(&[
    v::field("tenant", IDENTIFIER),
    v::field("olderThanMs", v::int().min(0.0).max(86_400_000.0)),
    v::field("limit", v::int().min(1.0).max(1_000.0)),
]);
static ARCHIVE_DELIVERIES: Method = Method::new("internal.pizza.archive", |ctx, input| {
    let tenant = input.text("tenant").unwrap_or_default();
    tenant_config(ctx, tenant)?;
    let cutoff = ctx.now()? - number(&input, "olderThanMs");
    let limit = number(&input, "limit");
    let queue = DELIVERIES.scope(tenant);
    let mut count = 0.0;
    for job in queue.scan(ctx)? {
        if count >= limit {
            break;
        }
        if job.text("state") != Some("completed") || number(&job, "updatedAt") > cutoff {
            continue;
        }
        let payload = job.get("payload");
        let key = order_id(payload.get("shop"), payload.get("orderId"));
        let order = ctx.get(&ORDERS, &key)?;
        if order.text("status") != Some("delivered") {
            continue;
        }
        let tally = ctx.get(&ARCHIVED, order.get("shop"))?;
        let archived = object! {
            "orders" => tally.number("orders").unwrap_or(0.0) + 1.0,
            "pizzas" => tally.number("pizzas").unwrap_or(0.0) + number(&order, "quantity"),
        };
        ctx.set(&ARCHIVED, order.get("shop"), archived)?;
        ctx.delete(&ORDERS, &key)?;
        queue.cancel(ctx, job.text("id").unwrap_or_default())?;
        count += 1.0;
    }
    Ok(object! {"archived" => count})
})
.args(&ARCHIVE_ARGS);

fn inspect(ctx: &mut Ctx, id: Value) -> Result<Value> {
    ctx.get_derived(&SHOP_SUMMARY, &id)
}
static INSPECT_SHOP: Method = Method::new("internal.pizza.shop", inspect).args(&SHOP_REF);
// Browsing can use a replica's coherent applied snapshot without contacting the
// leader. This preview may lag; stock checks and money updates stay in mutations.
static INSPECT_SHOP_LOCAL: Method =
    Method::new("internal.pizza.shop.local", inspect).args(&SHOP_REF);

// This deliberately public audit method returns raw business records as well as
// derived summaries, so a benchmark can independently verify every invariant.
static INSPECT_WORLD: Method = Method::new("internal.pizza.world", |ctx, _| {
    let settings = config(ctx)?;
    let shops: Vec<Value> = ctx.scan(&SHOPS)?.into_iter().map(|row| row.value).collect();
    let orders: Vec<Value> = ctx.scan(&ORDERS)?.into_iter().map(|row| row.value).collect();
    let tenant_ids: Vec<Value> = settings.get("tenantIds").as_array().cloned().unwrap_or_default();
    let mut jobs = Vec::new();
    for tenant in &tenant_ids {
        jobs.extend(DELIVERIES.scope(tenant.as_str().unwrap_or_default()).scan(ctx)?);
    }
    let timers = OVENS.scan(ctx, None)?;
    let mut summaries = Vec::new();
    for id in settings.get("shopIds").as_array().into_iter().flatten() {
        summaries.push(ctx.get_derived(&SHOP_SUMMARY, id)?);
    }
    let mut leaderboards = Map::new();
    for tenant in &tenant_ids {
        let ranking = ctx.get_derived(&LEADERBOARD, tenant)?;
        leaderboards.insert(tenant.as_str().unwrap_or_default(), ranking);
    }
    Ok(object! {
        "config" => settings, "shops" => shops, "orders" => orders, "jobs" => jobs, "timers" => timers,
        "summaries" => summaries, "leaderboards" => leaderboards,
    })
})
.args(&v::null());

// One public value drives the entire dashboard. Stable object keys let SSE
// patches address one order/job instead of shifting a table's array indexes.
// Keep the 120 orders with the latest activity on screen, so fresh orders,
// pizzas out of the oven and deliveries all show at any pace, with every
// delivery and oven timer still in flight. Summaries and totals, including
// archived orders, still cover the whole world. This observational view
// tolerates replication lag, including older code and aliases. Use pizza.world
// for a fresh audit; actions validate current state.
fn activity(order: &Value) -> f64 {
    order
        .number("deliveredAt")
        .or_else(|| order.number("readyAt"))
        .unwrap_or_else(|| number(order, "createdAt"))
}
fn with_archived(mut shop: Value, tally: &Value) -> Value {
    if tally.is_null() {
        return shop;
    }
    let (orders, pizzas) = (number(tally, "orders"), number(tally, "pizzas"));
    for (field, extra) in [
        ("orders", orders),
        ("delivered", orders),
        ("orderedQuantity", pizzas),
        ("deliveredQuantity", pizzas),
    ] {
        let total = number(&shop, field) + extra;
        shop.set(field, total);
    }
    shop
}
const DASHBOARD_ARGS: Schema = v::object(&[v::field("tenant", IDENTIFIER)]);
static INSPECT_DASHBOARD: Method = Method::new("internal.pizza.dashboard", |ctx, input| {
    let tenant = input.text("tenant").unwrap_or_default();
    let settings = tenant_config(ctx, tenant)?;
    let shop_ids: Vec<Value> = settings.get("shopIds").as_array().cloned().unwrap_or_default();
    let mut tallies = Vec::new();
    for id in &shop_ids {
        tallies.push(ctx.get(&ARCHIVED, id)?);
    }
    let mut summaries = Vec::new();
    for (id, tally) in shop_ids.iter().zip(&tallies) {
        summaries.push(with_archived(ctx.get_derived(&SHOP_SUMMARY, id)?, tally));
    }
    let archived: f64 = tallies.iter().map(|tally| tally.number("orders").unwrap_or(0.0)).sum();
    let mut recent = Vec::new();
    for id in &shop_ids {
        recent.extend(ctx.query(&ORDERS.by("byShop").eq(id.clone()))?);
    }
    recent.sort_by(|a, b| {
        let difference = activity(b) - activity(a);
        if difference != 0.0 && !difference.is_nan() {
            return if difference < 0.0 { core::cmp::Ordering::Less } else { core::cmp::Ordering::Greater };
        }
        by_key(a, b)
    });
    recent.truncate(120);
    let tenant_ids = config(ctx)?.get("tenantIds").clone();
    let by_key_map = |items: &[Value], key: &str| -> Map {
        items.iter().map(|item| (item.text(key).unwrap_or_default(), item.clone())).collect()
    };
    let jobs: Vec<Value> = DELIVERIES
        .scope(tenant)
        .scan(ctx)?
        .into_iter()
        .filter(|job| job.text("state") != Some("completed"))
        .collect();
    let timers: Vec<Value> = OVENS
        .scan(ctx, None)?
        .into_iter()
        .filter(|timer| timer.text("handler") == Some("bake") && timer.get("args").get("shop").at(0).as_str() == Some(tenant))
        .collect();
    let ranking = ctx.get_derived(&LEADERBOARD, &Value::from(tenant))?;
    let leaderboard: Vec<Value> = ranking.as_array().into_iter().flatten().map(|shop| shop.get("key").clone()).collect();
    let mut totals = [0.0; 7];
    for shop in &summaries {
        for (total, field) in totals.iter_mut().zip(["orders", "baking", "ready", "delivered", "deliveredQuantity", "revenue", "tips"]) {
            *total += number(shop, field);
        }
    }
    let [orders, baking, ready, delivered, pizzas, revenue, tips] = totals;
    Ok(object! {
        "tenant" => tenant, "tenantIds" => tenant_ids, "config" => settings,
        "summaries" => by_key_map(&summaries, "key"), "orders" => by_key_map(&recent, "key"),
        "jobs" => by_key_map(&jobs, "id"), "timers" => by_key_map(&timers, "id"), "leaderboard" => leaderboard,
        "totals" => object! {
            "orders" => orders, "baking" => baking, "ready" => ready, "delivered" => delivered,
            "pizzas" => pizzas, "revenue" => revenue, "tips" => tips,
        },
        "archived" => archived,
    })
})
.args(&DASHBOARD_ARGS);

pub static APP: App = App {
    uses: &[&OVENS, &DELIVERIES],
    collections: &[&ORDERS],
    definitions: &[
        Definition::Derived(&ORDER_STATS),
        Definition::Derived(&SHOP_SUMMARY),
        Definition::Derived(&LEADERBOARD),
    ],
    triggers: &[],
    http: &[
        ("pizza.setup", Definition::Mutation(&SETUP)),
        ("pizza.order", Definition::Mutation(&PLACE_ORDER)),
        ("pizza.claim", Definition::Mutation(&CLAIM_DELIVERY)),
        ("pizza.deliver", Definition::Mutation(&DELIVER_PIZZA)),
        ("pizza.tip", Definition::Mutation(&TIP_KITCHEN)),
        ("pizza.archive", Definition::Mutation(&ARCHIVE_DELIVERIES)),
        (
            "pizza.shop",
            Definition::Query(&INSPECT_SHOP, Consistency::Linearizable),
        ),
        (
            "pizza.shop.local",
            Definition::Query(&INSPECT_SHOP_LOCAL, Consistency::ReplicaLocal),
        ),
        (
            "pizza.world",
            Definition::Query(&INSPECT_WORLD, Consistency::Linearizable),
        ),
        (
            "pizza.dashboard",
            Definition::Query(&INSPECT_DASHBOARD, Consistency::ReplicaLocal),
        ),
    ],
};

flower_sdk::export!(APP);
