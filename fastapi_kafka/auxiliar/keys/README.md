# JWT keys

`private_key.pem` is **not** committed to git (see `.gitignore`).

Generate keys locally before first `docker compose up`:

```bash
cd fastapi_kafka/auxiliar
python generate_rsa_keys.py
```

This creates `private_key.pem` and `public_key.pem` in this folder.
