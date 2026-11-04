// =====================================================================================
// Part 1 - explore quickbite.orders in mongosh          (YOU write the parts marked TODO)
// =====================================================================================
// Open mongosh:   docker exec -it mongo mongosh quickbite
// Then copy ONE block at a time from this file into mongosh, run it, and compare with the
// "expected" comment. (You can also run the whole file:
//   docker exec -it mongo mongosh quickbite --file /part1_mongo/explore.js )
//
// Expected numbers are for the fresh seed (500 docs) BEFORE the simulators run.
// If your numbers are bigger, the simulators already added live orders - that is OK for
// the "shape" questions; for exact numbers re-run the seed (handout Part 0.4).
//
// Stuck? see solution/part1_mongo/explore.js
// =====================================================================================

db = db.getSiblingDB("quickbite");

// -------------------------------------------------------------------------------------
// 1.1 First five minutes on an unknown collection   (PROVIDED - just run it)
// -------------------------------------------------------------------------------------
db.orders.countDocuments({});                       // expected: 500
db.orders.findOne({ order_id: "QB-100000" });       // read the field names!
db.orders.distinct("status");                       // expected: 14 values (!)

// -------------------------------------------------------------------------------------
// 1.2 Find the data problems  (find at least 5)
// -------------------------------------------------------------------------------------
// a) $type profile of `total` (PROVIDED - use it as a template for b)
db.orders.aggregate([
  { $group: { _id: { $type: "$total" }, n: { $sum: 1 } } },
  { $sort: { n: -1 } }
]);
// expected: decimal 323, double 120, string 39, missing 18

// b) TODO (Part 1.2b): the same profile for `created_at`
// expected: date 427, string 43, long 30


// c) TODO (Part 1.2c): how many orders have NO status field at all?  (hint: $exists)
// expected: 18


// d) TODO (Part 1.2d): count per schema_v, then count documents that have customer.email
//    and documents that have customer.contact.email
// expected: schema_v 1 -> 241, schema_v 2 -> 259 ; customer.email 241 ; customer.contact.email 259


// e) TODO (Part 1.2e): orders with an EMPTY items array (hint: $size)
//    and orders where at least one items.qty is a string (hint: $type works on array elements)
// expected: 10 empty ; 85 with a string qty


// f) TODO (Part 1.2f): how many DIFFERENT spellings of delivery_address.city are there?
//    (hint: distinct(...).length)
// expected: 23


// g) TODO (Part 1.2g): which order_id values appear more than once?
//    (hint: $group by order_id with a count, then $match count > 1)
// expected: QB-100040 and QB-100041, 2 each


// -------------------------------------------------------------------------------------
// 1.3 CRUD fix: status casing
// -------------------------------------------------------------------------------------
db.orders.countDocuments({ status: "delivered" });  // expected: 334 - but how many are REALLY delivered?

// TODO (Part 1.3): ONE updateMany that lower-cases every status that is not lower-case yet.
//   - filter: status is a string AND status != its lower-case version
//       { status: { $type: "string" }, $expr: { $ne: ["$status", { $toLower: "$status" }] } }
//   - update: a PIPELINE update (note the [ ]):  [ { $set: { status: ..., updated_at: "$$NOW" } } ]
//   Why also set updated_at? -> handout Part 1.3
// expected: matchedCount 57, modifiedCount 57   (run it twice: the 2nd time modifiedCount is 0)


db.orders.countDocuments({ status: "delivered" });  // expected after the fix: 374
db.orders.distinct("status");                       // expected: 5 values


// -------------------------------------------------------------------------------------
// 1.4 Aggregations  (run AFTER the fix in 1.3)
// -------------------------------------------------------------------------------------
// a) TODO (Part 1.4a): number of orders per status, biggest first
// expected: delivered 374, cancelled 50, created 24, picked_up 20, null 18, accepted 14


// b) TODO (Part 1.4b): revenue per city for DELIVERED orders.
//    total has 4 types -> $sum: { $convert: { input: "$total", to: "decimal", onError: 0, onNull: 0 } }
//    round it with $set: { revenue: { $round: ["$revenue", 2] } }, sort by revenue desc
// expected: 23 groups; first: 'Tel Aviv' 70 orders 11010.00 ; second: 'Herzliya' 36 orders 10522.00


// c) TODO (Part 1.4c): same idea, but group by { $toLower: { $trim: { input: "$delivery_address.city" } } }
//    and only count orders. How many groups are left? Is it fixed?
// expected: 18 groups; 'tel aviv' 98 (but 'tlv', 'tel-aviv', 'tel aviv-yafo' are still separate)


// d) TODO (Part 1.4d): top 5 restaurants by revenue (delivered), group by restaurant_id + restaurant_name
// expected: R-15 Marina Fish 27 / 8634.00, R-03 Pizza Yafo 28 / 5622.00, R-16 Pituah Poke 25 / 4902.00,
//           R-18 Hadar Schnitzel 19 / 4551.00, R-13 Holon Home Food 18 / 4343.00


// e) TODO (Part 1.4e): top 5 dishes by quantity (delivered orders). $unwind the items array,
//    group by items.dish, $sum: { $toInt: "$items.qty" }   (why $toInt? -> 1.2e)
//    sort { qty: -1, _id: 1 }
// expected: Pizza Funghi 90, Schnitzel Plate 72, Garlic Bread 66, Sabich 60, Shawarma Laffa 58


// f) TODO (Part 1.4f): how many delivered orders arrived AFTER promised_at?
//    hint: { status: "delivered", $expr: { $gt: ["$delivered_at", "$promised_at"] } }
// expected: 117


// -------------------------------------------------------------------------------------
// 1.5 OPTIONAL bonus - indexes and explain()  (only if you covered indexes)
// -------------------------------------------------------------------------------------
// The Airflow DAG (Part 4) asks MongoDB every hour: { updated_at: { $gt: <watermark> } }.
// TODO (bonus): run
//     db.orders.find({ updated_at: { $gt: ISODate("2026-10-29T00:00:00Z") } }).explain("executionStats")
// and look at winningPlan.stage. Then db.orders.createIndex({ updated_at: 1 }) and explain again.
// expected: COLLSCAN before; IXSCAN (under FETCH) after, totalDocsExamined 88 = nReturned 88
//   (88 if you ran the 1.3 fix - it moved updated_at to NOW on 57 documents; 38 if you skipped it.
//    That is exactly how the hourly DAG will notice your fix!)
