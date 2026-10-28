import os
import json
import threading
import boto3
from boto3.dynamodb.conditions import Key, And, Or
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from app.config import settings

LOCAL_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "local_db.json")
_db_lock = threading.Lock()

def is_aws_configured() -> bool:
    key = settings.aws_access_key_id.strip() if settings.aws_access_key_id else ""
    return bool(key and not key.startswith("your_aws_"))

def _load_local_db() -> dict:
    with _db_lock:
        if os.path.exists(LOCAL_DB_PATH):
            try:
                with open(LOCAL_DB_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

def _save_local_db(db: dict):
    with _db_lock:
        os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
        with open(LOCAL_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, default=str)

def _eval_condition(cond, item: dict) -> bool:
    if cond is None:
        return True
    if isinstance(cond, And):
        v1, v2 = cond._values
        return _eval_condition(v1, item) and _eval_condition(v2, item)
    if isinstance(cond, Or):
        v1, v2 = cond._values
        return _eval_condition(v1, item) or _eval_condition(v2, item)
    if hasattr(cond, 'expression_operator'):
        op = cond.expression_operator
        vals = cond._values
        attr_name = vals[0].name if hasattr(vals[0], 'name') else str(vals[0])
        val = vals[1] if len(vals) > 1 else None
        item_val = item.get(attr_name)
        if op == '=':
            return item_val == val
        elif op == '<>':
            return item_val != val
        elif op == '<':
            return item_val is not None and item_val < val
        elif op == '<=':
            return item_val is not None and item_val <= val
        elif op == '>':
            return item_val is not None and item_val > val
        elif op == '>=':
            return item_val is not None and item_val >= val
        elif op == 'attribute_exists':
            return attr_name in item
        elif op == 'attribute_not_exists':
            return attr_name not in item
        elif op == 'contains':
            return val in item_val if item_val else False
        elif op == 'begins_with':
            return item_val.startswith(val) if item_val else False
    return True

def _local_put_item(table_name: str, item: dict):
    db = _load_local_db()
    table = db.setdefault(table_name, {})
    item_id = str(item.get("id", item.get("email", len(table))))
    table[item_id] = item
    _save_local_db(db)

def _local_get_item(table_name: str, key: dict):
    db = _load_local_db()
    table = db.get(table_name, {})
    for item in table.values():
        if all(item.get(k) == v for k, v in key.items()):
            return item
    return None

def _local_scan_items(table_name: str, filter_expression=None):
    db = _load_local_db()
    table = db.get(table_name, {})
    results = []
    for item in table.values():
        if _eval_condition(filter_expression, item):
            results.append(item)
    return results

def _local_update_item(table_name: str, key: dict, update_expression: str, expression_values: dict, expression_names: dict = None):
    db = _load_local_db()
    table = db.setdefault(table_name, {})
    target_id = None
    target_item = None
    for i_id, item in table.items():
        if all(item.get(k) == v for k, v in key.items()):
            target_id = i_id
            target_item = item
            break
    if target_item is None:
        target_id = str(key.get("id", len(table)))
        target_item = dict(key)
        table[target_id] = target_item

    expression_names = expression_names or {}
    expr = update_expression.strip()
    if expr.upper().startswith("SET "):
        expr = expr[4:].strip()
    parts = [p.strip() for p in expr.split(",")]
    for part in parts:
        if "=" in part:
            lhs, rhs = part.split("=", 1)
            lhs = lhs.strip()
            rhs = rhs.strip()
            attr = expression_names.get(lhs, lhs)
            val = expression_values.get(rhs, rhs)
            target_item[attr] = val

    _save_local_db(db)
    return target_item

def _local_delete_item(table_name: str, key: dict):
    db = _load_local_db()
    table = db.get(table_name, {})
    to_delete = [i_id for i_id, item in table.items() if all(item.get(k) == v for k, v in key.items())]
    for i_id in to_delete:
        del table[i_id]
    _save_local_db(db)

def get_dynamodb():
    return boto3.resource(
        "dynamodb",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

def get_table(table_name: str):
    return get_dynamodb().Table(table_name)

def put_item(table_name: str, item: dict):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            table.put_item(Item=item)
            return
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB put_item failed ({e}), using local storage fallback")
    _local_put_item(table_name, item)

def get_item(table_name: str, key: dict):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            response = table.get_item(Key=key)
            return response.get("Item")
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB get_item failed ({e}), using local storage fallback")
    return _local_get_item(table_name, key)

def query_items(table_name: str, key_condition, index_name: str = None):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            kwargs = {"KeyConditionExpression": key_condition}
            if index_name:
                kwargs["IndexName"] = index_name
            response = table.query(**kwargs)
            return response.get("Items", [])
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB query_items failed ({e}), using local storage fallback")
    return _local_scan_items(table_name, key_condition)

def scan_items(table_name: str, filter_expression=None):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            kwargs = {}
            if filter_expression:
                kwargs["FilterExpression"] = filter_expression
            items = []
            while True:
                response = table.scan(**kwargs)
                items.extend(response.get("Items", []))
                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    break
                kwargs["ExclusiveStartKey"] = last_key
            return items
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB scan_items failed ({e}), using local storage fallback")
    return _local_scan_items(table_name, filter_expression)

def update_item(table_name: str, key: dict, update_expression: str, expression_values: dict, expression_names: dict = None):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            kwargs = {
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeValues": expression_values,
                "ReturnValues": "ALL_NEW",
            }
            if expression_names:
                kwargs["ExpressionAttributeNames"] = expression_names
            response = table.update_item(**kwargs)
            return response.get("Attributes")
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB update_item failed ({e}), using local storage fallback")
    return _local_update_item(table_name, key, update_expression, expression_values, expression_names)

def delete_item(table_name: str, key: dict):
    if is_aws_configured():
        try:
            table = get_table(table_name)
            table.delete_item(Key=key)
            return
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            print(f"DynamoDB delete_item failed ({e}), using local storage fallback")
    _local_delete_item(table_name, key)
