# Source data contracts - version 1

CSV fields are stored as text in Phase 1 and validated using the types below.
A later typed transformation layer will explicitly cast them. UTC timestamps
use ISO 8601 with a `Z` suffix. Raw records also contain `_batch_id`,
`_ingested_at` and `_source`.

## Products

File: `data/raw/<batch>/products/products.jsonl`

Grain: one record per catalogue product. Primary key: `id`.

| Field | Meaning / validation |
|---|---|
| `id` | Positive integer product ID |
| `title` | Nonempty product title |
| `category` | Nonempty category |
| `price` | Finite nonnegative numeric value |
| Other API fields | Preserved; optional for Phase 1 |

Original response payloads are retained in the adjacent `pages` directory.
Prices are synthetic; no claim is made about the API's currency. Product IDs in
the transaction simulator must exist in the exact catalogue snapshot used.

## Customers

File: `data/raw/<batch>/customers/customers.csv`

Grain: one record per synthetic customer. Primary key: `customer_id`.

| Field | Meaning / validation |
|---|---|
| `customer_id` | Nonempty unique identifier, generated as `C0000001` |
| `customer_name` | Fictional name, such as `Customer 0000001` |
| `city` | Synthetic location category |
| `country` | `US` in the supplied simulator |
| `created_at` | UTC source creation timestamp |
| `updated_at` | UTC source update timestamp, not before creation |

No real people or personal data are included.

## Orders

File: `data/raw/<batch>/orders/orders.csv`

Grain: one record per order header. Primary key: `order_id`.
Foreign key: `customer_id -> customers.customer_id`.

| Field | Meaning / validation |
|---|---|
| `order_id` | Unique identifier, generated as `O000000001` |
| `customer_id` | Must exist in the customers table |
| `order_date` | UTC timestamp at or after customer creation |
| `status` | `placed`, `shipped`, `delivered` or `cancelled` |
| `currency` | `USD`, an explicit simulator convention |
| `order_total` | Nonnegative two-decimal string; sum of the order's item amounts |
| `updated_at` | UTC source update timestamp, not before the order date |

Order total excludes taxes, delivery fees, discounts and refunds in version 1.
Even a cancelled order retains its originally generated order value. An
all-status total is not recognized revenue. Any later sales metric must state
which statuses it includes.

## Order items

File: `data/raw/<batch>/order_items/order_items.csv`

Grain: one product line within an order. Primary key: `order_item_id`.
Additional unique grain: `(order_id, line_number)`.
Foreign keys: `order_id -> orders.order_id`, `product_id -> products.id`.

| Field | Meaning / validation |
|---|---|
| `order_item_id` | Unique identifier, generated as `OI0000000001` |
| `order_id` | Must reference an existing order |
| `line_number` | Positive integer, unique within the order |
| `product_id` | Positive integer, must reference an existing product |
| `quantity` | Positive integer |
| `unit_price` | Nonnegative two-decimal string, representing the purchase-time price |
| `updated_at` | UTC source update timestamp, not before the order date |

The simulator creates 1-4 distinct product lines per order and quantity 1-5 per
line. The validator requires valid positive values rather than hard-coding every
simulator range as a permanent business rule.

`line_total = quantity * unit_price` will be a derived transformation field.
The raw order items do not carry a repeated order-header total. This distinction
prevents double-counting during later fact-table modeling.

## Failure policy

Version 1 fails fast on invalid structure, duplicate keys, missing relationships,
invalid types/timestamps and inconsistent totals. It does not silently drop,
fill, repair or aggregate raw source records. Bad-record quarantine and explicit
cleaning rules will be introduced and tested separately in Phase 2.
