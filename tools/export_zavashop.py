#!/usr/bin/env python3
"""Read only the four ZavaShop source literals; do not import or execute upstream modules."""
import argparse
import ast
import json
from pathlib import Path

DATA_FILES = {
    'inventory': ('inventory', '_INVENTORY'),
    'supplier': ('supplier', '_SUPPLIERS'),
    'shipping': ('shipping', '_CARRIER_RATES'),
    'pricing': ('pricing', '_PRICES'),
}
ALLOWED_CALLS = {'StockLocation', 'SupplierOption', 'CarrierRate', 'PriceRow', 'frozenset', 'set'}


def literal(node):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Dict):
        return {literal(k): literal(v) for k, v in zip(node.keys, node.values)}
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        values = [literal(x) for x in node.elts]
        return tuple(values) if isinstance(node, ast.Tuple) else values
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        val = literal(node.operand)
        if type(val) not in (int, float):
            raise ValueError('Unsupported negative value')
        return -val
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ALLOWED_CALLS:
        if node.func.id in ('frozenset', 'set'):
            if len(node.args) != 1 or node.keywords:
                raise ValueError('Unsupported set call')
            return sorted(literal(node.args[0]))
        if node.args or any(k.arg is None for k in node.keywords):
            raise ValueError('Expected keyword-only record constructor')
        return {k.arg: literal(k.value) for k in node.keywords}
    raise ValueError(f'Unsupported source expression: {ast.dump(node)[:160]}')


def read_assignment(path, name):
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    matches = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            matches.append(node.value)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == name:
            matches.append(node.value)
    if len(matches) != 1:
        raise ValueError(f'{path}: expected one {name} assignment; got {len(matches)}')
    return literal(matches[0])


def export_data(repo):
    root = repo / 'src' / 'mcp_servers'
    if not root.is_dir():
        raise FileNotFoundError(f'Expected ZavaShop src/mcp_servers at {root}')
    source = {k: read_assignment(root / folder / 'store.py', variable) for k, (folder, variable) in DATA_FILES.items()}
    inv = []
    for sku, locations in source['inventory'].items():
        for loc, rec in locations.items():
            if rec['location_id'] != loc:
                raise ValueError(f'Inventory location mismatch for {sku}: {loc}')
            inv.append({'sku': sku, **rec})
    offers = [{'sku': sku, **rec} for sku, rows in source['supplier'].items() for rec in rows]
    carriers = []
    for rec in source['shipping']:
        carriers.append({**rec, 'destination_store_ids': sorted(rec['destination_store_ids'])})
    prices = [{'sku': sku, 'location_id': loc, **rec} for (sku, loc), rec in source['pricing'].items()]
    result = {'format_version': 1, 'inventory_positions': inv, 'supplier_offers': offers,
              'carrier_services': carriers, 'retail_prices': prices}
    validate(result)
    return result


def validate(data):
    assert data['format_version'] == 1
    for table in ('inventory_positions', 'supplier_offers', 'carrier_services', 'retail_prices'):
        if not isinstance(data[table], list) or not data[table]:
            raise ValueError(f'Empty or malformed {table}')
    for row in data['inventory_positions']:
        for key in ('sku', 'location_id', 'on_hand', 'reserved', 'reorder_point', 'days_until_stockout'):
            if key not in row: raise ValueError(f'Missing {key} in inventory')
        if any(type(row[k]) is not int or row[k] < 0 for k in ('on_hand','reserved','reorder_point','days_until_stockout')):
            raise ValueError('Invalid inventory numeric values')
    for row in data['supplier_offers']:
        for key in ('sku','supplier_id','name','available_qty','unit_cost','lead_time_days','reliability_score'):
            if key not in row: raise ValueError(f'Missing {key} in supplier')
    for row in data['carrier_services']:
        for key in ('carrier','service_level','base_cost','per_unit_cost','eta_days','destination_store_ids'):
            if key not in row: raise ValueError(f'Missing {key} in shipping')
    for row in data['retail_prices']:
        for key in ('sku','location_id','current_price','unit_cost','floor_price','ceiling_price','category'):
            if key not in row: raise ValueError(f'Missing {key} in pricing')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path, help='Path to existing ZavaShop checkout')
    parser.add_argument('--output', required=True, type=Path, help='Output JSON file')
    args = parser.parse_args()
    data = export_data(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(f"Exported: {len(data['inventory_positions'])} inventory, {len(data['supplier_offers'])} offers, {len(data['carrier_services'])} carrier services, {len(data['retail_prices'])} prices -> {args.output}")

if __name__ == '__main__':
    main()
