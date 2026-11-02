import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('export_zavashop', ROOT / 'tools/export_zavashop.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def original():
    return json.loads((ROOT / 'data/seed/zavashop_original.json').read_text(encoding='utf-8'))


def test_original_counts_and_integrity():
    data = original()
    mod.validate(data)
    assert len(data['inventory_positions']) == 6
    assert len(data['supplier_offers']) == 4
    assert len(data['carrier_services']) == 3
    assert len(data['retail_prices']) == 3
    assert len({(r['sku'],r['location_id']) for r in data['inventory_positions']}) == 6
    assert len({(r['sku'],r['supplier_id']) for r in data['supplier_offers']}) == 4
    assert len({(r['sku'],r['location_id']) for r in data['retail_prices']}) == 3


def test_inventory_example_and_rates():
    data = original()
    example = next(r for r in data['inventory_positions'] if r['sku']=='ZS-1042' and r['location_id']=='store-101')
    assert (example['on_hand'],example['reserved'],example['reorder_point'],example['days_until_stockout']) == (6,4,24,2)
    assert example['on_hand'] - example['reserved'] == 2
    freight = next(r for r in data['carrier_services'] if r['carrier']=='Northwind Freight')
    assert freight['base_cost'] == 180.0 and freight['per_unit_cost'] == 0.42
    assert set(freight['destination_store_ids']) == {'store-101', 'store-202'}


def test_scenario_references_are_in_original():
    original_data=original()
    extra=json.loads((ROOT / 'data/seed/additional_test_scenarios.json').read_text(encoding='utf-8'))
    products={r['sku'] for r in original_data['inventory_positions']}
    locations={r['location_id'] for r in original_data['inventory_positions']}
    carriers={(r['carrier'],r['service_level']) for r in original_data['carrier_services']}
    for key in ('inbound_inventory','inventory_policies','demand_history','pricing_history'):
        for row in extra[key]:
            assert row['sku'] in products and row['location_id'] in locations
    for key in ('location_transfer_rules','shipping_lanes'):
        for row in extra[key]:
            assert row['origin_id'] in locations and row['destination_id'] in locations
    for row in extra['shipping_lanes']:
        assert (row['carrier'],row['service_level']) in carriers


def test_export_rejects_executable_expressions():
    import ast
    for source in ('__import__("os")', 'open("/tmp/foo")', 'x + 1'):
        try:
            mod.literal(ast.parse(source,mode='eval').body)
        except ValueError:
            pass
        else:
            raise AssertionError(f'Expression should be rejected: {source}')


def test_no_provenance_columns_in_schema():
    schema=(ROOT/'infra/postgres/schema.sql').read_text(encoding='utf-8').lower()
    for absent in ('source_type','dataset_id','source_reference','is_synthetic'):
        assert absent not in schema


def test_existing_source_export_matches_bundled_snapshot_when_available():
    # Set ZAVASHOP_SOURCE in the test environment to validate your own clone.
    import os
    location=os.environ.get('ZAVASHOP_SOURCE')
    if location:
        assert mod.export_data(Path(location))==original(), 'Local checkout has diverged from snapshot. Re-export before load.'
