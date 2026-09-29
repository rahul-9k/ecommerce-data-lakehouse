"""Pure synthetic fixtures shared by unit and isolated integration tests."""
from datetime import datetime, timezone
from copy import deepcopy
from phase3_fixture import rows_for
from src.incremental.contracts import make_demo_events


def set_demo_statuses(rows):
    rows['orders'][0]['status']='shipped'
    rows['orders'][1]['status']='delivered'
    statuses={r['order_id']:r['status'] for r in rows['orders']}
    for item in rows['order_items']:
        item['order_status']=statuses[item['order_id']]


def demo_rows():
    rows=rows_for('baseline_fixture')
    set_demo_statuses(rows)
    return rows


def demo_events(batch='delta_test',parent=0,new_orders=2,rows=None):
    rows=deepcopy(rows or demo_rows())
    items={o['order_id']:[i for i in rows['order_items'] if i['order_id']==o['order_id']] for o in rows['orders']}
    return make_demo_events(batch,parent,rows['products'],rows['customers'],rows['orders'][:1],rows['orders'][1:2],
        items,new_orders,max(o['updated_at'] for o in rows['orders']),datetime(2026,9,29,tzinfo=timezone.utc))
