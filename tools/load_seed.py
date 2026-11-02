#!/usr/bin/env python3
"""Initialize the local PostgreSQL retail schema and idempotently upsert seed records."""
import argparse
import json
import os
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def get_data(path):
    obj = json.loads(Path(path).read_text(encoding='utf-8'))
    if obj.get('format_version') != 1:
        raise ValueError(f'{path}: unsupported format_version')
    return obj


def upsert(cur, stmt, params):
    cur.execute(stmt, params)


def load(cur, doc):
    source_lists = ['inventory_positions', 'supplier_offers', 'carrier_services', 'retail_prices',
                    'inbound_inventory', 'inventory_policies', 'location_transfer_rules',
                    'demand_history', 'shipping_lanes', 'pricing_history']
    for label in source_lists:
        if label in doc and not isinstance(doc[label], list):
            raise ValueError(f'{label} must be a list')
    products = set()
    locations = set()
    for row in doc.get('inventory_positions', []):
        products.add(row['sku']); locations.add(row['location_id'])
    for row in doc.get('supplier_offers', []):
        products.add(row['sku'])
    for row in doc.get('retail_prices', []):
        products.add(row['sku']); locations.add(row['location_id'])
    for row in doc.get('carrier_services', []):
        locations.update(row['destination_store_ids'])
    for name in ('inbound_inventory', 'inventory_policies', 'demand_history', 'pricing_history'):
        for row in doc.get(name, []):
            products.add(row['sku']); locations.add(row['location_id'])
    for name in ('location_transfer_rules','shipping_lanes'):
        for row in doc.get(name, []):
            locations.update((row['origin_id'],row['destination_id']))
    for row in doc.get('locations', []):
        locations.add(row['location_id'])
    for sku in sorted(products):
        upsert(cur, 'INSERT INTO retail.products(sku) VALUES (%s) ON CONFLICT DO NOTHING', (sku,))
    for loc in sorted(locations):
        upsert(cur, 'INSERT INTO retail.locations(location_id) VALUES (%s) ON CONFLICT DO NOTHING', (loc,))
    for row in doc.get('locations', []):
        upsert(cur, 'UPDATE retail.locations SET location_type=%s WHERE location_id=%s', (row.get('location_type'),row['location_id']))
    for row in doc.get('inventory_positions', []):
        upsert(cur, '''INSERT INTO retail.inventory_positions(sku,location_id,on_hand,reserved,reorder_point,days_until_stockout)
           VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(sku,location_id) DO UPDATE SET
           on_hand=EXCLUDED.on_hand,reserved=EXCLUDED.reserved,reorder_point=EXCLUDED.reorder_point,
           days_until_stockout=EXCLUDED.days_until_stockout''',
           tuple(row[k] for k in ('sku','location_id','on_hand','reserved','reorder_point','days_until_stockout')))
    for row in doc.get('supplier_offers', []):
        upsert(cur, '''INSERT INTO retail.suppliers(supplier_id,name) VALUES(%s,%s)
          ON CONFLICT(supplier_id) DO UPDATE SET name=EXCLUDED.name''', (row['supplier_id'],row['name']))
        upsert(cur, '''INSERT INTO retail.supplier_offers(sku,supplier_id,available_qty,unit_cost,lead_time_days,reliability_score)
          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(sku,supplier_id) DO UPDATE SET
          available_qty=EXCLUDED.available_qty,unit_cost=EXCLUDED.unit_cost,
          lead_time_days=EXCLUDED.lead_time_days,reliability_score=EXCLUDED.reliability_score''',
          (row['sku'], row['supplier_id'], row['available_qty'], Decimal(str(row['unit_cost'])),
           row['lead_time_days'],Decimal(str(row['reliability_score']))))
    for row in doc.get('carrier_services', []):
        sid = carrier_id(cur,row['carrier'],row['service_level'])
        upsert(cur, '''INSERT INTO retail.shipping_rates(service_id,base_cost,per_unit_cost,eta_days)
          VALUES(%s,%s,%s,%s) ON CONFLICT(service_id) DO UPDATE SET base_cost=EXCLUDED.base_cost,
          per_unit_cost=EXCLUDED.per_unit_cost,eta_days=EXCLUDED.eta_days''',
          (sid,Decimal(str(row['base_cost'])),Decimal(str(row['per_unit_cost'])),row['eta_days']))
        for dst in row['destination_store_ids']:
            upsert(cur, '''INSERT INTO retail.carrier_destinations(service_id,destination_id)
              VALUES(%s,%s) ON CONFLICT DO NOTHING''', (sid,dst))
    for row in doc.get('retail_prices', []):
        upsert(cur, '''INSERT INTO retail.retail_prices(sku,location_id,current_price,unit_cost,floor_price,ceiling_price,category)
          VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(sku,location_id) DO UPDATE SET
          current_price=EXCLUDED.current_price,unit_cost=EXCLUDED.unit_cost,
          floor_price=EXCLUDED.floor_price,ceiling_price=EXCLUDED.ceiling_price,category=EXCLUDED.category''',
          (row['sku'],row['location_id'],Decimal(str(row['current_price'])),Decimal(str(row['unit_cost'])),
           Decimal(str(row['floor_price'])),Decimal(str(row['ceiling_price'])),row['category']))
        upsert(cur, 'UPDATE retail.products SET category=%s WHERE sku=%s', (row['category'],row['sku']))
    for row in doc.get('inbound_inventory', []):
        upsert(cur, '''INSERT INTO retail.inbound_inventory(inbound_id,sku,location_id,quantity,expected_date,status)
          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(inbound_id) DO UPDATE SET
          sku=EXCLUDED.sku,location_id=EXCLUDED.location_id,quantity=EXCLUDED.quantity,
          expected_date=EXCLUDED.expected_date,status=EXCLUDED.status''',
          tuple(row.get(k) for k in ('inbound_id','sku','location_id','quantity','expected_date','status')))
    for row in doc.get('inventory_policies', []):
        upsert(cur, '''INSERT INTO retail.inventory_policies(sku,location_id,target_stock,safety_stock)
          VALUES(%s,%s,%s,%s) ON CONFLICT(sku,location_id) DO UPDATE SET
          target_stock=EXCLUDED.target_stock,safety_stock=EXCLUDED.safety_stock''',
          tuple(row[k] for k in ('sku','location_id','target_stock','safety_stock')))
    for row in doc.get('location_transfer_rules', []):
        upsert(cur, '''INSERT INTO retail.location_transfer_rules(origin_id,destination_id,eligible,transit_days)
          VALUES(%s,%s,%s,%s) ON CONFLICT(origin_id,destination_id) DO UPDATE SET
          eligible=EXCLUDED.eligible,transit_days=EXCLUDED.transit_days''',
          (row['origin_id'],row['destination_id'],row['eligible'],row.get('transit_days')))
    for row in doc.get('demand_history', []):
        upsert(cur, '''INSERT INTO retail.demand_history(sku,location_id,demand_date,units_sold)
          VALUES(%s,%s,%s,%s) ON CONFLICT(sku,location_id,demand_date) DO UPDATE SET
          units_sold=EXCLUDED.units_sold''',
          tuple(row[k] for k in ('sku','location_id','demand_date','units_sold')))
    for row in doc.get('shipping_lanes', []):
        sid = carrier_id(cur,row['carrier'],row['service_level'])
        upsert(cur, '''INSERT INTO retail.shipping_lanes(origin_id,destination_id,service_id,eligible)
          VALUES(%s,%s,%s,%s) ON CONFLICT(origin_id,destination_id,service_id)
          DO UPDATE SET eligible=EXCLUDED.eligible''',
          (row['origin_id'],row['destination_id'],sid,row['eligible']))
    for row in doc.get('pricing_history', []):
        upsert(cur, '''INSERT INTO retail.pricing_history(sku,location_id,effective_date,price)
          VALUES(%s,%s,%s,%s) ON CONFLICT(sku,location_id,effective_date)
          DO UPDATE SET price=EXCLUDED.price''',
          (row['sku'],row['location_id'],row['effective_date'],Decimal(str(row['price']))))


def carrier_id(cur, carrier, service_level):
    cur.execute('''INSERT INTO retail.carrier_services(carrier,service_level)
      VALUES(%s,%s) ON CONFLICT(carrier,service_level) DO UPDATE SET carrier=EXCLUDED.carrier
      RETURNING service_id''',(carrier,service_level))
    return cur.fetchone()[0]


def counts(cur):
    tables = ['products','locations','inventory_positions','suppliers','supplier_offers',
              'carrier_services','shipping_rates','carrier_destinations','retail_prices',
              'inbound_inventory','inventory_policies','location_transfer_rules',
              'demand_history','shipping_lanes','pricing_history']
    results={}
    for t in tables:
        cur.execute(f'SELECT count(*) FROM retail.{t}')
        results[t]=cur.fetchone()[0]
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--seed', default=str(ROOT / 'data/seed/zavashop_original.json'))
    p.add_argument('--extra', default=None, help='Optional test scenario fixture JSON')
    p.add_argument('--database-url', default=os.environ.get('DATABASE_URL'))
    args = p.parse_args()
    if not args.database_url:
        p.error('Set DATABASE_URL or pass --database-url (avoid putting secrets in shell history)')
    import psycopg
    docs=[get_data(args.seed)] + ([get_data(args.extra)] if args.extra else [])
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute((ROOT / 'infra/postgres/schema.sql').read_text(encoding='utf-8'))
            for doc in docs:
                load(cur,doc)
            result=counts(cur)
    for name, num in result.items():
        print(f'{name:25s} {num:>6}')
    print('Committed successfully (repeatable UPSERT operation).')

if __name__ == '__main__':
    main()
