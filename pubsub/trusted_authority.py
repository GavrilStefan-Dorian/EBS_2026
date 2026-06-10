import hashlib
import hmac
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
SECRET_KEY = b"ebs_super_secret_key"

# In-memory store to reverse the hashes (act as decryption)
decryption_store = {}

class EncryptRequest(BaseModel):
    value: str

class EncryptResponse(BaseModel):
    encrypted_value: str

class DecryptRequest(BaseModel):
    encrypted_value: str

class DecryptResponse(BaseModel):
    value: str

@app.post("/encrypt", response_model=EncryptResponse)
def encrypt_value(req: EncryptRequest):
    """
    Returns a deterministic HMAC-SHA256 hash of the input value.
    Deterministic hashing allows both equality (==) and inequality (!=)
    comparisons by the broker without exposing the underlying plaintext.
    """
    h = hmac.new(SECRET_KEY, req.value.encode('utf-8'), hashlib.sha256)
    encrypted = h.hexdigest()
    decryption_store[encrypted] = req.value
    return EncryptResponse(encrypted_value=encrypted)

@app.post("/decrypt", response_model=DecryptResponse)
def decrypt_value(req: DecryptRequest):
    """
    Reverses the hash back to the original plaintext for the subscriber.
    """
    original = decryption_store.get(req.encrypted_value, req.encrypted_value)
    return DecryptResponse(value=original)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("trusted_authority:app", host="127.0.0.1", port=8000, reload=False)
