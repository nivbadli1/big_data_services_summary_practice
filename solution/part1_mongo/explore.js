// =====================================================================================
// SOLUTION - Part 1: explore quickbite.orders in mongosh
// =====================================================================================
// You can run the whole file at once to compare with your answers:
//     docker exec -it mongo mongosh quickbite --file /part1_mongo_solution/explore.js
// NOTE: step 1.3 MODIFIES data (it fixes the status casing). Running it again is safe:
// the second time it modifies 0 documents.
//
// Expected numbers are for the deterministic seed (500 docs) BEFORE the simulators run.
// =====================================================================================

db = db.getSiblingDB("quickbite");
function title(t) { print("\n=== " + t + " ==="); }

// -------------------------------------------------------------------------------------
// 1.1 First five minutes on an unknown collection
// -------------------------------------------------------------------------------------
title("1.1a count");
printjson(db.orders.countDocuments({}));                       // 500

title("1.1b one document - read its field names");
printjson(db.orders.findOne({ order_id: "QB-100000" }));

title("1.1c distinct status");
printjson(db.orders.distinct("status"));                       // 14 spellings of 5 statuses

// -------------------------------------------------------------------------------------
// 1.2 Find the data problems
// -------------------------------------------------------------------------------------
// WHY $type: MongoDB is schema-on-read. The same field can hold a different BSON type in
// every document. Your pipeline will crash (or silently drop rows) on the type it did
// not expect - so profile the types BEFORE writing any transformation.
title("1.2a $type profile of total");
printjson(db.orders.aggregate([
  { $group: { _id: { $type: "$total" }, n: { $sum: 1 } } },
  { $sort: { n: -1 } }
]).toArray());

title("1.2b $type profile of created_at");
printjson(db.orders.aggregate([
  { $group: { _id: { $type: "$created_at" }, n: { $sum: 1 } } },
  { $sort: { n: -1 } }
]).toArray());

title("1.2c missing status");
printjson(db.orders.countDocuments({ status: { $exists: false } }));   // 18

title("1.2d schema versions and where the email lives");
printjson(db.orders.aggregate([
  { $group: { _id: "$schema_v", n: { $sum: 1 } } }, { $sort: { _id: 1 } }
]).toArray());
printjson(db.orders.countDocuments({ "customer.email": { $exists: true } }));          // v1
printjson(db.orders.countDocuments({ "customer.contact.email": { $exists: true } }));  // v2

title("1.2e empty items arrays, qty stored as a string");
printjson(db.orders.countDocuments({ items: { $size: 0 } }));
printjson(db.orders.countDocuments({ "items.qty": { $type: "string" } }));

title("1.2f city spellings");
printjson(db.orders.distinct("delivery_address.city").length);

title("1.2g duplicated order_id");
printjson(db.orders.aggregate([
  { $group: { _id: "$order_id", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
  { $sort: { _id: 1 } }
]).toArray());

// -------------------------------------------------------------------------------------
// 1.3 CRUD fix: status casing
// -------------------------------------------------------------------------------------
// The "silent exclusion" problem: WHERE status = 'delivered' misses 'DELIVERED' and
// 'Delivered'. Nobody gets an error - the numbers are just wrong.
title("1.3a before the fix");
printjson(db.orders.countDocuments({ status: "delivered" }));

// Pipeline-style update (the [ ] around $set): the new value is COMPUTED from the old one.
// Filter: only strings that are not already lower-case -> modifiedCount = what we really fixed.
// We also bump updated_at: every write must move updated_at, otherwise the incremental
// extract in Part 4 (updated_at > watermark) would never see this change.
title("1.3b the fix");
printjson(db.orders.updateMany(
  { status: { $type: "string" }, $expr: { $ne: ["$status", { $toLower: "$status" }] } },
  [{ $set: { status: { $toLower: "$status" }, updated_at: "$$NOW" } }]
));

title("1.3c after the fix");
printjson(db.orders.countDocuments({ status: "delivered" }));
printjson(db.orders.distinct("status"));

// -------------------------------------------------------------------------------------
// 1.4 Aggregations (GROUP BY as a pipeline)
// -------------------------------------------------------------------------------------
title("1.4a orders per status");
printjson(db.orders.aggregate([
  { $group: { _id: "$status", orders: { $sum: 1 } } },
  { $sort: { orders: -1 } }
]).toArray());

// $convert with onError/onNull: total can be Decimal128, double, string or missing.
// to: "decimal" keeps money exact (no 0.1 + 0.2 = 0.30000000000000004).
// onNull: 0 -> a MISSING total counts as 0 here. In the real pipeline (Part 4) we do NOT
// do that silently - we quarantine those rows instead. Here we only want a quick look.
title("1.4b revenue per city (delivered orders, raw city spelling)");
printjson(db.orders.aggregate([
  { $match: { status: "delivered" } },
  { $group: {
      _id: "$delivery_address.city",
      orders: { $sum: 1 },
      revenue: { $sum: { $convert: { input: "$total", to: "decimal", onError: 0, onNull: 0 } } }
  } },
  { $set: { revenue: { $round: ["$revenue", 2] } } },
  { $sort: { revenue: -1 } }
]).toArray());

title("1.4c same, but with a lower-cased + trimmed city (how many groups remain?)");
printjson(db.orders.aggregate([
  { $match: { status: "delivered" } },
  { $group: { _id: { $toLower: { $trim: { input: "$delivery_address.city" } } }, orders: { $sum: 1 } } },
  { $sort: { orders: -1 } }
]).toArray());

title("1.4d top 5 restaurants by revenue (delivered)");
printjson(db.orders.aggregate([
  { $match: { status: "delivered" } },
  { $group: {
      _id: { id: "$restaurant_id", name: "$restaurant_name" },
      orders: { $sum: 1 },
      revenue: { $sum: { $convert: { input: "$total", to: "decimal", onError: 0, onNull: 0 } } }
  } },
  { $set: { revenue: { $round: ["$revenue", 2] } } },
  { $sort: { revenue: -1 } },
  { $limit: 5 }
]).toArray());

// $unwind: one output document per array element (like exploding a list column).
// qty is sometimes "2" (a string) -> $toInt before summing, otherwise $sum ignores it!
title("1.4e top 5 dishes by quantity ($unwind)");
printjson(db.orders.aggregate([
  { $match: { status: "delivered" } },
  { $unwind: "$items" },
  { $group: { _id: "$items.dish", qty: { $sum: { $toInt: "$items.qty" } } } },
  { $sort: { qty: -1, _id: 1 } },
  { $limit: 5 }
]).toArray());

title("1.4f late deliveries ($expr compares two fields of the same document)");
printjson(db.orders.countDocuments({ status: "delivered", $expr: { $gt: ["$delivered_at", "$promised_at"] } }));

// -------------------------------------------------------------------------------------
// 1.5 OPTIONAL bonus: index + explain()
// -------------------------------------------------------------------------------------
title("1.5 bonus: explain before / after an index on updated_at");
const q = { updated_at: { $gt: ISODate("2026-10-29T00:00:00Z") } };
print("before: " + db.orders.find(q).explain("executionStats").queryPlanner.winningPlan.stage);
db.orders.createIndex({ updated_at: 1 });
const after = db.orders.find(q).explain("executionStats");
print("after:  " + after.queryPlanner.winningPlan.stage + " -> " + after.queryPlanner.winningPlan.inputStage.stage);
print("docs examined: " + after.executionStats.totalDocsExamined + ", returned: " + after.executionStats.nReturned);
// -> 88 / 88: the 38 orders really updated after 29 Oct + the 57 orders our 1.3 fix touched
//    (minus 7 that are in both groups). Our fix moved updated_at -> the next DAG run re-reads them.
