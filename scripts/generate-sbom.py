#!/usr/bin/env python3
"""Generate and validate Tranfi's minimal SPDX SBOM."""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import re
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def read_cjson_version(root: Path) -> str:
    header = (root / 'src/cJSON.h').read_text(encoding='utf-8')
    parts = {}
    for name in ['MAJOR', 'MINOR', 'PATCH']:
        m = re.search(rf'#define CJSON_VERSION_{name}\s+(\d+)', header)
        if not m:
            raise SystemExit(f'cJSON.h missing CJSON_VERSION_{name}')
        parts[name] = m.group(1)
    return f"{parts['MAJOR']}.{parts['MINOR']}.{parts['PATCH']}"


def validate(root: Path, provenance: dict) -> None:
    version = read_cjson_version(root)
    if version != provenance.get('version'):
        raise SystemExit(f'cJSON provenance version {provenance.get("version")} does not match cJSON.h {version}')
    c_file = (root / 'src/cJSON.c').read_text(encoding='utf-8')
    if 'static CJSON_THREAD_LOCAL error global_error' not in c_file:
        raise SystemExit('Tranfi cJSON thread-local parse-error patch is missing')
    if any((root / path).exists() for path in ['src/cJSON_Utils.c', 'src/cJSON_Utils.h']):
        raise SystemExit('cJSON_Utils files must not be vendored/compiled in Tranfi core')


def make_sbom(root: Path, provenance: dict) -> dict:
    now = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    return {
        'spdxVersion': 'SPDX-2.3',
        'dataLicense': 'CC0-1.0',
        'SPDXID': 'SPDXRef-DOCUMENT',
        'name': 'tranfi-sbom',
        'documentNamespace': 'https://github.com/tranfi/tranfi/sbom/tranfi',
        'creationInfo': {
            'created': now,
            'creators': ['Tool: tranfi scripts/generate-sbom.py'],
        },
        'packages': [
            {
                'name': 'tranfi',
                'SPDXID': 'SPDXRef-Package-tranfi',
                'downloadLocation': 'https://github.com/tranfi/tranfi',
                'filesAnalyzed': False,
                'licenseDeclared': 'Apache-2.0',
                'licenseConcluded': 'Apache-2.0',
                'copyrightText': 'Copyright (c) 2026 Anton Zemlyansky and contributors',
            },
            {
                'name': provenance['name'],
                'SPDXID': 'SPDXRef-Package-cJSON',
                'versionInfo': provenance['version'],
                'downloadLocation': f"{provenance['source']}@{provenance['tag']}",
                'filesAnalyzed': False,
                'licenseDeclared': provenance['license'],
                'licenseConcluded': provenance['license'],
                'copyrightText': 'Copyright (c) 2009-2017 Dave Gamble and cJSON contributors',
                'externalRefs': [
                    {
                        'referenceCategory': 'PACKAGE-MANAGER',
                        'referenceType': 'purl',
                        'referenceLocator': f"pkg:github/DaveGamble/cJSON@{provenance['tag']}",
                    }
                ],
            },
        ],
        'relationships': [
            {
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relationshipType': 'DESCRIBES',
                'relatedSpdxElement': 'SPDXRef-Package-tranfi',
            },
            {
                'spdxElementId': 'SPDXRef-Package-tranfi',
                'relationshipType': 'CONTAINS',
                'relatedSpdxElement': 'SPDXRef-Package-cJSON',
            },
        ],
        'annotations': [
            {
                'annotationDate': now,
                'annotationType': 'OTHER',
                'annotator': 'Tool: tranfi scripts/generate-sbom.py',
                'SPDXID': 'SPDXRef-Annotation-cJSON-local-patches',
                'subject': 'SPDXRef-Package-cJSON',
                'comment': '; '.join(provenance.get('local_patches', [])),
            }
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', default='build/tranfi-sbom.spdx.json')
    args = parser.parse_args()

    root = repo_root()
    provenance = json.loads((root / 'third_party/cjson.json').read_text(encoding='utf-8'))
    validate(root, provenance)
    sbom = make_sbom(root, provenance)
    out = root / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(sbom, indent=2) + '\n', encoding='utf-8')
    print(f'generate-sbom: wrote {out.relative_to(root)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
