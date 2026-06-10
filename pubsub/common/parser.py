import re
import uuid
import time
import requests
from datetime import datetime
from proto import messages_pb2
import os

TA_URL_ENCRYPT = "http://127.0.0.1:8000/encrypt"
TA_URL_DECRYPT = "http://127.0.0.1:8000/decrypt"

_encrypt_cache = {}
_decrypt_cache = {}

def use_encryption():
    return os.environ.get("USE_ENCRYPTION", "1") == "1"

def encrypt_string(val):
    """Contacts the Trusted Authority to encrypt string fields deterministically."""
    if val in _encrypt_cache:
        return _encrypt_cache[val]
    try:
        resp = requests.post(TA_URL_ENCRYPT, json={"value": val}, timeout=2)
        if resp.status_code == 200:
            encrypted = resp.json()["encrypted_value"]
            _encrypt_cache[val] = encrypted
            return encrypted
    except Exception:
        pass
    return val

def decrypt_string(val):
    """Contacts the Trusted Authority to decrypt string fields back to plaintext."""
    if val in _decrypt_cache:
        return _decrypt_cache[val]
    try:
        resp = requests.post(TA_URL_DECRYPT, json={"encrypted_value": val}, timeout=2)
        if resp.status_code == 200:
            decrypted = resp.json()["value"]
            _decrypt_cache[val] = decrypted
            return decrypted
    except Exception:
        pass
    return val

def parse_value(field, val_str):
    val_str = val_str.strip()

    if field == "date":
        val = val_str.strip('"')
        return float(datetime.strptime(val, "%d.%m.%Y").toordinal()), False

    if val_str.startswith('"') and val_str.endswith('"'):
        return val_str.strip('"'), True

    try:
        return float(val_str), False
    except ValueError:
        return val_str, True

def parse_publication_line(line, pub_id=None, publisher_id=""):
    """
    Parses: {(company,"Amazon");(value,36.79);(drop,1.92);(variation,0.884);(date,"25.04.2023")}
    Into a protobuf Publication.
    """
    line = line.strip()
    if not line.startswith("{") or not line.endswith("}"):
        return None
    
    content = line[1:-1]
    pairs = content.split(");(")
    
    pub = messages_pb2.Publication()
    pub.id = pub_id or str(uuid.uuid4())
    pub.publisher_id = publisher_id
    pub.timestamp = int(time.time() * 1000)
    
    for pair in pairs:
        pair = pair.replace("(", "").replace(")", "")
        parts = pair.split(",", 1)
        if len(parts) == 2:
            key = parts[0].strip()
            val_str = parts[1].strip()
            val, is_str = parse_value(key, val_str)
            if is_str:
                pub.string_fields[key] = encrypt_string(val) if use_encryption() else val
            else:
                pub.double_fields[key] = val
                
    return pub

def parse_subscription_line(line, sub_id=None, subscriber_id=""):
    """
    Parses: {(company,=,"Amazon");(value,>,344.78);(drop,=,37.68)}
    Into a protobuf Subscription.
    """
    line = line.strip()
    if not line.startswith("{") or not line.endswith("}"):
        return None
        
    content = line[1:-1]
    triplets = content.split(");(")
    
    sub = messages_pb2.Subscription()
    sub.id = sub_id or str(uuid.uuid4())
    sub.subscriber_id = subscriber_id
    
    for triplet in triplets:
        triplet = triplet.replace("(", "").replace(")", "")
        parts = triplet.split(",", 2)
        if len(parts) == 3:
            key = parts[0].strip()
            op = parts[1].strip()
            val_str = parts[2].strip()
            
            val, is_str = parse_value(key, val_str)
            
            cond = sub.conditions.add()
            cond.field = key
            cond.operator = op
            cond.is_string = is_str
            if is_str:
                cond.string_value = encrypt_string(val) if use_encryption() else val
            else:
                cond.double_value = val
                
    return sub
