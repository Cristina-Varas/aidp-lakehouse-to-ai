#!/usr/bin/env python3
"""Generate local wrapping keys and recover credentials without printing secrets.

Requires Python 3 and cryptography. No database credentials are accepted by CLI.
"""
import argparse
import json
import os
from pathlib import Path
import re
import stat

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

USERS = {"LAKEHOUSE_DEMO", "LAKEHOUSE_DEMO_RO"}


def write_private(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(value)


def secure_directory(path):
    path = path.resolve()
    repo = Path(__file__).resolve().parents[2]
    if path == repo or repo in path.parents:
        raise ValueError("Secret directory must be outside the repository")
    if path.stat().st_uid != os.getuid() or stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise ValueError("Secret directory must be owned by the current user with mode 0700")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "public", "recover"])
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    if args.action == "init":
        args.directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    directory = secure_directory(args.directory)
    keyfile = directory / "private-key.pem"
    if args.action == "init":
        key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        write_private(keyfile, key.private_bytes(serialization.Encoding.PEM,
                      serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        print("Local wrapping key created; use the public action for the SQL substitution.")
        return
    if keyfile.stat().st_uid != os.getuid() or stat.S_IMODE(keyfile.stat().st_mode) != 0o600:
        raise ValueError("Private key must be owned by the current user with mode 0600")
    key = serialization.load_pem_private_key(keyfile.read_bytes(), password=None)
    if args.action == "public":
        pem = key.public_key().public_bytes(serialization.Encoding.PEM,
                                           serialization.PublicFormat.SubjectPublicKeyInfo)
        # DBMS_CRYPTO expects ASCII base64, not DER or the complete PEM.
        print(b"".join(pem.splitlines()[1:-1]).hex().upper())
        return
    if args.receipt is None:
        raise ValueError("recover requires --receipt (the ciphertext-only MCP result or SQLcl output)")
    destination = directory / "credentials.json"
    credentials = {}
    if destination.exists():
        if destination.is_symlink() or stat.S_IMODE(destination.stat().st_mode) != 0o600:
            raise ValueError("Existing credentials file must be a regular mode-0600 file")
        credentials = json.loads(destination.read_text())
    matches = re.findall(r"CREDENTIAL\|(LAKEHOUSE_DEMO(?:_RO)?)\|([A-Fa-f0-9]{768})",
                         args.receipt.read_text())
    if not matches:
        raise ValueError("No wrapped credential receipts found")
    for username, ciphertext in matches:
        secret = key.decrypt(bytes.fromhex(ciphertext), padding.OAEP(
            mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)).decode("utf-8")
        if username not in USERS or not re.fullmatch(r"aZ9#[A-F0-9]{48}", secret):
            raise ValueError("Invalid credential payload")
        if username in credentials and credentials[username] != secret:
            raise ValueError("Refusing to replace an existing local credential")
        credentials[username] = secret
    # Atomic replacement avoids truncating previously recovered credentials.
    temporary = directory / "credentials.pending"
    write_private(temporary, (json.dumps(credentials, indent=2) + "\n").encode())
    os.replace(temporary, destination)
    print("Credentials recovered to a local mode-0600 file; no passwords displayed.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # A library exception must never expose decrypted values or private key data.
        raise SystemExit("Local secret operation failed; no secret values displayed.") from None
