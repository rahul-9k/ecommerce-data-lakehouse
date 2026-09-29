"""Version 1 source contracts. Raw files retain these fields plus ingestion metadata."""
CUSTOMER_COLUMNS = (
    "customer_id", "customer_name", "city", "country", "created_at", "updated_at",
)
ORDER_COLUMNS = (
    "order_id", "customer_id", "order_date", "status", "currency", "order_total", "updated_at",
)
ORDER_ITEM_COLUMNS = (
    "order_item_id", "order_id", "line_number", "product_id", "quantity", "unit_price", "updated_at",
)
CSV_CONTRACTS = {
    "customers": CUSTOMER_COLUMNS,
    "orders": ORDER_COLUMNS,
    "order_items": ORDER_ITEM_COLUMNS,
}
ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled")
