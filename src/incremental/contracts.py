"""Small, explicit contract for the educational order-change feed.

A feed is an immutable, contiguous sequence of complete order envelopes. The
sequence is supplied by our simulator, NOT a database CDC log position. Products
and customers are fixed reference snapshots. Existing order lines cannot be
added, removed, re-keyed, or moved by this version of the contract.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any

from src.common import PipelineError, validate_batch_id
from src.load.warehouse_contracts import CONTRACTS, KEYS, Schemas

INCREMENTAL_SCHEMAS = Schemas('landing_inc', 'analytics_inc', 'pipeline_inc')
STATUSES = {'placed', 'shipped', 'delivered', 'cancelled'}
KINDS = {'insert', 'status_update', 'quantity_correction'}
FEED_SOURCE = 'phase4_simulated_order_change_feed'
MAX_EVENTS = 5000


def ensure_disjoint(baseline: Schemas, target: Schemas) -> None:
    original = {baseline.landing, baseline.analytics, baseline.meta}
    new = {target.landing, target.analytics, target.meta}
    if original & new:
        raise PipelineError('Phase 4 must use different schemas from the immutable Phase 3 baseline.')


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise PipelineError('Feed timestamps must explicitly include a timezone.')
        return value.astimezone(timezone.utc).isoformat(timespec='microseconds')
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise PipelineError('Non-finite amount.')
        return format(value, '.2f')
    raise TypeError(f'Cannot serialize {type(value).__name__}')


def canonical(event: dict) -> str:
    return json.dumps(event, default=json_value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


def event_digest(event: dict) -> str:
    return hashlib.sha256(canonical(event).encode('utf-8')).hexdigest()


def timestamp(value: Any) -> datetime:
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError('Missing timezone')
        return dt.astimezone(timezone.utc)
    except (TypeError, AttributeError, ValueError) as exc:
        raise PipelineError('Feed timestamp must be an ISO-8601 value with timezone.') from exc


def parse_row(table: str, value: dict) -> dict:
    columns = CONTRACTS[table]
    if not isinstance(value, dict) or set(value) != {c for c, _, _ in columns}:
        raise PipelineError(f'{table}: fields differ from the declared contract.')
    result = {}
    for col, kind, _ in columns:
        x = value[col]
        if kind == 'string':
            if not isinstance(x, str) or not x.strip() or '\x00' in x:
                raise PipelineError(f'{table}.{col}: invalid text.')
        elif kind in ('int', 'bigint'):
            if type(x) is not int:
                raise PipelineError(f'{table}.{col}: expected an integer, not a string/bool/float.')
        elif kind == 'timestamp':
            x = timestamp(x)
        else:
            # JSON amounts are strings, never binary floating-point numbers.
            if not isinstance(x, (str, Decimal)):
                raise PipelineError(f'{table}.{col}: amounts must be decimal strings.')
            try:
                x = Decimal(x)
                if not x.is_finite() or x < 0 or x != x.quantize(Decimal('0.01')):
                    raise ValueError('Invalid amount')
            except (InvalidOperation, ValueError) as exc:
                raise PipelineError(f'{table}.{col}: invalid nonnegative two-decimal amount.') from exc
        result[col] = x
    return result


def parse_event(value: dict, batch_id: str) -> dict:
    validate_batch_id(batch_id)
    if not isinstance(value, dict) or set(value) != {'change_seq', 'event_id', 'kind', 'order', 'items'}:
        raise PipelineError('Unexpected change-event fields.')
    seq = value['change_seq']
    if type(seq) is not int or seq < 1 or seq > 2**63 - 1:
        raise PipelineError('change_seq must be a positive signed 64-bit integer.')
    if value['event_id'] != f'{batch_id}:{seq}' or value['kind'] not in KINDS:
        raise PipelineError('Invalid event ID or change kind.')
    order = parse_row('orders', value['order'])
    if not isinstance(value['items'], list) or not value['items']:
        raise PipelineError('A change must carry every line of a nonempty order.')
    items = [parse_row('order_items', row) for row in value['items']]
    if order['currency'] != 'USD' or order['status'] not in STATUSES:
        raise PipelineError('Unsupported currency or order status.')
    if order['updated_at'] < order['order_date']:
        raise PipelineError('Order update precedes the order date.')
    if (order['order_year'], order['order_month']) != (order['order_date'].year, order['order_date'].month):
        raise PipelineError('Order partition fields disagree with its UTC date.')
    if len({i['order_item_id'] for i in items}) != len(items):
        raise PipelineError('Duplicate order-item ID inside an event.')
    if sorted(i['line_number'] for i in items) != list(range(1, len(items) + 1)):
        raise PipelineError('Order line numbers must be unique and contiguous from 1.')
    for row in [order, *items]:
        if row['_batch_id'] != batch_id or row['_source'] != FEED_SOURCE:
            raise PipelineError('Change-row provenance does not match this feed.')
    for item in items:
        if item['quantity'] < 1 or item['product_id'] < 1:
            raise PipelineError('Quantity and product ID must be positive.')
        if item['line_amount'] != item['quantity'] * item['unit_price']:
            raise PipelineError('Incorrect line amount.')
        for field in ('order_id', 'customer_id', 'order_date', 'currency', 'order_year', 'order_month', 'updated_at'):
            if item[field] != order[field]:
                raise PipelineError(f'Line/order context differs: {field}.')
        if item['order_status'] != order['status']:
            raise PipelineError('Line/order context differs: status.')
    if sum((i['line_amount'] for i in items), Decimal('0.00')) != order['order_total']:
        raise PipelineError('Order total differs from the sum of its complete set of lines.')
    return dict(change_seq=seq, event_id=value['event_id'], kind=value['kind'], order=order,
                items=sorted(items, key=lambda i: i['line_number']))


def validate_events(values: list[dict], batch_id: str, parent_seq: int) -> list[dict]:
    if type(parent_seq) is not int or parent_seq < 0:
        raise PipelineError('Invalid previous sequence.')
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_EVENTS:
        raise PipelineError(f'A feed must have 1 to {MAX_EVENTS} events.')
    events = [parse_event(v, batch_id) for v in values]
    if [e['change_seq'] for e in events] != list(range(parent_seq + 1, parent_seq + 1 + len(events))):
        raise PipelineError('Feed sequence has a gap, duplicate, or out-of-order event.')
    if len({e['order']['order_id'] for e in events}) != len(events):
        raise PipelineError('This feed format allows only one change per order per batch.')
    all_ids = [i['order_item_id'] for e in events for i in e['items']]
    if len(all_ids) != len(set(all_ids)):
        raise PipelineError('An order-item ID appears in more than one change event.')
    return events


def check_update(event: dict, old_order: dict | None, old_items: list[dict]) -> None:
    """Reject stale/re-keyed/incomplete updates before issuing any upserts."""
    order = event['order']
    if old_order is None:
        if event['kind'] != 'insert' or old_items:
            raise PipelineError('An update refers to an unknown order.')
        return
    if event['kind'] == 'insert':
        raise PipelineError('An insert refers to an existing order.')
    if order['updated_at'] <= max([old_order['updated_at'], *(i['updated_at'] for i in old_items)]):
        raise PipelineError('Stale or equal-version update rejected; watermark is not advanced.')
    for col in ('order_id', 'customer_id', 'order_date', 'currency'):
        if order[col] != old_order[col]:
            raise PipelineError(f'This change contract does not modify existing {col}.')
    old_by_id = {i['order_item_id']: i for i in old_items}
    if set(old_by_id) != {i['order_item_id'] for i in event['items']}:
        raise PipelineError('Existing lines cannot be omitted, added, deleted, or re-keyed.')
    for item in event['items']:
        old = old_by_id[item['order_item_id']]
        for col in ('order_id', 'line_number', 'product_id'):
            if item[col] != old[col]:
                raise PipelineError(f'This change contract does not modify a line {col}.')
    if event['kind'] == 'status_update':
        if order['status'] == old_order['status']:
            raise PipelineError('A status-update event must actually change status.')
        for item in event['items']:
            old = old_by_id[item['order_item_id']]
            if (item['quantity'], item['unit_price']) != (old['quantity'], old['unit_price']):
                raise PipelineError('A status update cannot change line prices/quantities.')
    if event['kind'] == 'quantity_correction':
        if order['status'] != old_order['status']:
            raise PipelineError('A quantity correction cannot also change order status.')
        if not any(i['quantity'] != old_by_id[i['order_item_id']]['quantity'] for i in event['items']):
            raise PipelineError('A quantity correction must actually change a quantity.')
        if any(i['unit_price'] != old_by_id[i['order_item_id']]['unit_price'] for i in event['items']):
            raise PipelineError('A quantity correction cannot change transaction unit prices.')


def make_demo_events(batch_id: str, parent_seq: int, products: list[dict], customers: list[dict],
                     status_orders: list[dict], correction_orders: list[dict], items_by_order: dict,
                     new_orders: int, source_time: datetime, ingestion_time: datetime) -> list[dict]:
    """Deterministic payloads for the given source state, time and demo parameters."""
    validate_batch_id(batch_id)
    if type(new_orders) is not int or not 0 <= new_orders <= 1000:
        raise PipelineError('Demo new-order count must be between 0 and 1000.')
    if not products or not customers:
        raise PipelineError('Reference products/customers are missing.')
    change_time = timestamp(source_time) + timedelta(days=1)
    meta = {'_batch_id': batch_id, '_ingested_at': timestamp(ingestion_time), '_source': FEED_SOURCE}
    events = []

    def add(order, items, kind):
        order.update(meta, updated_at=change_time)
        for item in items:
            item.update(meta, updated_at=change_time, order_status=order['status'])
        seq = parent_seq + len(events) + 1
        events.append(dict(change_seq=seq, event_id=f'{batch_id}:{seq}', kind=kind, order=order, items=items))

    for original in status_orders:
        order = deepcopy(original)
        if order['status'] not in ('placed', 'shipped'):
            raise PipelineError('Demo status changes need placed or shipped orders.')
        order['status'] = 'delivered'
        add(order, deepcopy(items_by_order[order['order_id']]), 'status_update')
    for original in correction_orders:
        order = deepcopy(original)
        items = deepcopy(items_by_order[order['order_id']])
        items[0]['quantity'] += 1
        items[0]['line_amount'] = items[0]['quantity'] * items[0]['unit_price']
        order['order_total'] = sum((i['line_amount'] for i in items), Decimal('0.00'))
        add(order, items, 'quantity_correction')
    for n in range(new_orders):
        order_id = f'INC_{batch_id}_{n + 1:06d}'
        customer = customers[n % len(customers)]
        order = dict(order_id=order_id, customer_id=customer['customer_id'], order_date=change_time,
                     status='placed', currency='USD', updated_at=change_time, order_year=change_time.year,
                     order_month=change_time.month, **meta)
        items = []
        for line in (1, 2):
            product = products[(n * 2 + line - 1) % len(products)]
            quantity = (n + line - 1) % 3 + 1
            price = product['catalog_price']
            items.append(dict(order_item_id=f'{order_id}_L{line}', order_id=order_id, line_number=line,
                product_id=product['product_id'], quantity=quantity, unit_price=price,
                updated_at=change_time, line_amount=price * quantity, customer_id=customer['customer_id'],
                order_date=change_time, order_status=order['status'], currency='USD',
                order_year=change_time.year, order_month=change_time.month, **meta))
        order['order_total'] = sum((i['line_amount'] for i in items), Decimal('0.00'))
        add(order, items, 'insert')
    # The round trip also verifies the public JSON representation, not just Python values.
    return validate_events(json.loads(json.dumps(events, default=json_value)), batch_id, parent_seq)
