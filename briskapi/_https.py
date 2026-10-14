"""The TLS context of briskapi's HTTPS requests (uploads, BRiSK sign-in and data, the cloud API).

Python from python.org on macOS loads no CA certificates of its own until its "Install Certificates" step has
run, and every certificate then fails to verify. Where Python has loaded none, the system's bundle is added, so
requests are still verified, against the certificates the operating system trusts."""
from __future__ import annotations

import functools
import os
import ssl

SYSTEM_BUNDLES = ('/etc/ssl/cert.pem',)   # macOS's, kept with the system; many Linux and BSD systems have one too


@functools.cache
def context() -> ssl.SSLContext:
    tls = ssl.create_default_context()
    if not tls.cert_store_stats()['x509_ca']:
        for bundle in SYSTEM_BUNDLES:
            if os.path.isfile(bundle):
                tls.load_verify_locations(bundle)
                break
    return tls
