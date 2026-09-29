"""Create a tiny Phase 2-format Parquet fixture; does not call Spark or the API."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from src.common import inventory, write_json
from src.load.warehouse_contracts import CONTRACTS, expected_schemas


def rows_for(batch: str) -> dict:
    def dt(value): return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    m = {"_batch_id": batch, "_ingested_at":dt("2026-09-29T10:00:00"), "_source":"phase3_handcrafted_fixture"}
    products = [dict(product_id=i, product_name=n, category=c, catalog_price=Decimal(p), **m) for i,n,c,p in
                [(1,"Keyboard","electronics","10.00"),(2,"Mouse","electronics","2.50"),(3,"Notebook","stationery","1.25")]]
    customers = [dict(customer_id=i, customer_name=n,city=city,country="US",
                      created_at=dt("2025-12-01T00:00:00"),updated_at=dt("2025-12-01T00:00:00"),**m)
                 for i,n,city in [("C1",'Demo, "One"',"Austin"),("C2","Demo\nTwo","Boston")]]
    orders = [dict(order_id=i,customer_id=c,order_date=dt(date),status=status,currency="USD",order_total=Decimal(total),
                   updated_at=dt(update),order_year=2026,order_month=month,**m)
              for i,c,date,status,total,update,month in
              [("O1","C1","2026-01-05T10:00:00","delivered","25.00","2026-01-06T10:00:00",1),
               ("O2","C2","2026-02-05T10:00:00","cancelled","6.25","2026-02-06T10:00:00",2)]]
    parents = {r['order_id']:r for r in orders}
    items=[]
    for item,order,line,product,q,price in [("I1","O1",1,1,2,"10.00"),("I2","O1",2,2,2,"2.50"),
                                         ("I3","O2",1,3,3,"1.25"),("I4","O2",2,2,1,"2.50")]:
        o=parents[order]
        items.append(dict(order_item_id=item,order_id=order,line_number=line,product_id=product,quantity=q,
                          unit_price=Decimal(price),updated_at=o['updated_at'],line_amount=Decimal(price)*q,
                          customer_id=o['customer_id'],order_date=o['order_date'],order_status=o['status'],currency="USD",
                          order_year=2026,order_month=o['order_month'],**m))
    return dict(products=products,customers=customers,orders=orders,order_items=items)


def make_snapshot(data_dir: Path, batch: str="fixture_001", change=None) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq
    rows = rows_for(batch)
    if change: change(rows)
    output=data_dir/'processed'/batch
    output.mkdir(parents=True)
    for table, columns in CONTRACTS.items():
        fields=[]
        for col,typ,_ in columns:
            dtype = (pa.string() if typ=='string' else pa.int64() if typ=='bigint' else pa.int32() if typ=='int'
                     else pa.timestamp('us') if typ=='timestamp' else pa.decimal128(int(typ[8:].split(',')[0]),2))
            fields.append(pa.field(col,dtype))
        schema=pa.schema(fields)
        typed=pa.Table.from_pylist(rows[table],schema=schema)
        (output/table).mkdir()
        if table in ('orders','order_items'):
            pq.write_to_dataset(typed,root_path=str(output/table),partition_cols=['order_year','order_month'],
                                use_deprecated_int96_timestamps=True)
        else:
            pq.write_table(typed,output/table/'part.parquet',use_deprecated_int96_timestamps=True)
    total=sum((r['order_total'] for r in rows['orders']),Decimal('0.00'))
    delivered=sum((r['order_total'] for r in rows['orders'] if r['status']=='delivered'),Decimal('0.00'))
    quality={"status":"passed","row_counts":{t:len(r) for t,r in rows.items()},
             "gross_order_value_usd_all_statuses":format(total,'.2f'),"delivered_order_value_usd":format(delivered,'.2f')}
    write_json(output/'quality_report.json',quality)
    write_json(output/'manifest.json',{"format_version":1,"phase":2,"status":"success","batch_id":batch,
        "output_id":batch,"source_mode":"offline_fixture","schemas":expected_schemas(),"quality":quality,
        "data_provenance":{"all_records":"Tiny handcrafted fixture for warehouse tests, not actual transactions"},
        "files":inventory(output)})
    return output
