# -*- coding: utf-8 -*-
"""package_dist.py - freeze a delivery tree into a distributable zip + checksums.

    python scripts/chaos/ctk/package_dist.py --tag 20260722

Given an assembled delivery tree (delivery) RecShop_<tag>/ (built by
build_full_delivery.py), this produces the release-ready distribution:

  (delivery) RecShop_<tag>/SHA256SUMS.txt        (per-file, in-tree, sha256sum -c compatible)
  (delivery) RecShop_<tag>_dist/RecShop_<tag>.zip
  (delivery) RecShop_<tag>_dist/RecShop_<tag>.zip.sha256

WHAT IT DOES (the order matters and is the whole point):
  1. Regenerate SHA256SUMS.txt covering EVERY file in the tree (incl. any
     LICENSE.md / DATASHEET.md), excluding the manifest itself.
  2. Zip the tree (so the zip CARRIES the manifest - a recipient who only gets
     the zip can still `sha256sum -c SHA256SUMS.txt` after unpacking).
  3. Hash the zip -> .zip.sha256 (recipient checks the download).
  4. Adversarially self-verify: pull real bytes back out of the zip and confirm
     they match what the manifest claims. A manifest that disagrees with its own
     tree is worse than none, so this MUST pass or the script errors non-zero.

Zips are large (~235MB) and gitignored; the tree itself is gitignored too. Only
the small text products (README/LICENSE/DATASHEET/SHA256SUMS/.zip.sha256) are
worth tracking. This script is idempotent: rerun after editing the tree.

Windows note: uses PowerShell Compress-Archive (zip/7z not on PATH here).
"""
import argparse
import hashlib
import os
import subprocess
import sys
import zipfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def sha_file(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for c in iter(lambda: f.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def sha_bytes(b):
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tag', required=True, help='delivery tag, e.g. 20260722')
    ap.add_argument('--name', default='RecShop', help='tree name prefix (default RecShop)')
    a = ap.parse_args()

    stem = '%s_%s' % (a.name, a.tag)
    base = os.path.join(ROOT, 'datasets', '_delivery', stem)
    if not os.path.isdir(base):
        sys.exit('[ERR] tree not found: %s (build it with build_full_delivery.py first)' % base)
    dist = os.path.join(ROOT, 'datasets', '_delivery', '%s_dist' % stem)
    os.makedirs(dist, exist_ok=True)
    manifest = os.path.join(base, 'SHA256SUMS.txt')
    zip_path = os.path.join(dist, '%s.zip' % stem)
    zipsum = os.path.join(dist, '%s.zip.sha256' % stem)

    # 1. per-file manifest (exclude itself)
    rows, n = [], 0
    for root, _, files in os.walk(base):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), base).replace(os.sep, '/')
            if rel == 'SHA256SUMS.txt':
                continue
            rows.append('%s  %s' % (sha_file(os.path.join(root, f)), rel))
            n += 1
            if n % 3000 == 0:
                print('  hashed %d...' % n)
    rows.sort(key=lambda r: r.split('  ', 1)[1])
    with open(manifest, 'w', encoding='utf-8', newline='\n') as fh:
        fh.write('\n'.join(rows) + '\n')
    print('[1/4] manifest: %d files -> %s' % (len(rows), os.path.relpath(manifest, ROOT)))

    # 2. zip (PowerShell Compress-Archive; zip carries the manifest)
    if os.path.exists(zip_path):
        os.remove(zip_path)
    ps = ("$ErrorActionPreference='Stop'; Compress-Archive -Path '%s' "
          "-DestinationPath '%s' -CompressionLevel Optimal" % (base, zip_path))
    r = subprocess.run(['powershell', '-NoProfile', '-Command', ps])
    if r.returncode or not os.path.exists(zip_path):
        sys.exit('[ERR] zip failed (rc=%s)' % r.returncode)
    print('[2/4] zip: %.1fMB -> %s' % (os.path.getsize(zip_path) / 1e6, os.path.relpath(zip_path, ROOT)))

    # 3. hash the zip
    zh = sha_file(zip_path)
    with open(zipsum, 'w', encoding='utf-8', newline='\n') as fh:
        fh.write('%s  %s.zip\n' % (zh, stem))
    print('[3/4] zip sha256: %s' % zh)

    # 4. adversarial self-verify (bytes from zip vs manifest claims)
    claims = dict((r.split('  ', 1)[1], r.split('  ', 1)[0]) for r in rows)
    prefix = '%s/' % stem
    with zipfile.ZipFile(zip_path) as zf:
        names = [x for x in zf.namelist() if not x.endswith('/')]

        def rel_of(x):
            nn = x.replace('\\', '/')
            p = nn.split('/', 1)
            return p[1] if len(p) > 1 and p[0] == stem else nn
        zrels = set(rel_of(x) for x in names)
        missing = zrels - set(claims) - {'SHA256SUMS.txt'}
        extra = set(claims) - zrels
        picks = [p for p in ('LICENSE.md', 'DATASHEET.md', 'MANIFEST.json', 'README.md') if p in claims]
        fail = 0
        for rel in picks:
            got = sha_bytes(zf.read(prefix + rel))
            if got != claims.get(rel):
                print('  SPOT MISMATCH %s' % rel)
                fail += 1
        if missing or extra or fail:
            sys.exit('[ERR] self-verify FAILED: missing=%d extra=%d spot_fail=%d'
                     % (len(missing), len(extra), fail))
    print('[4/4] self-verify OK: %d zip files, 0 missing, 0 extra, %d spot-checks match'
          % (len(zrels), len(picks)))
    print('DONE -> %s' % os.path.relpath(dist, ROOT))


if __name__ == '__main__':
    main()
