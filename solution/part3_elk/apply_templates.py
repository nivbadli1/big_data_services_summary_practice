"""
apply_templates.py - shortcut: send the 4 index templates from solution/part3_elk/mappings.http
to Elasticsearch without clicking through Kibana Dev Tools.

    docker compose exec apps python /solution/part3_elk/apply_templates.py

Use it if you are stuck in Part 0.5 / 2.0, or to reset the templates. It reads the
same file you would paste into Dev Tools, so the result is identical.
Templates only apply to NEW indices: if an index was created before its template,
delete it (DELETE <index> in Dev Tools) and let the consumer / app re-create it.
"""
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

ES = os.getenv("ES_URL", "http://elasticsearch:9200")
HTTP_FILE = Path(__file__).with_name("mappings.http")


def parse_templates(text):
    """Yield (path, body_dict) for every 'PUT _index_template/...' request in a Dev Tools file."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        m = re.match(r"^PUT\s+(_index_template/\S+)", lines[i])
        i += 1
        if not m:
            continue
        body, depth, started = [], 0, False
        while i < len(lines):
            line = lines[i]
            i += 1
            if not started and not line.strip().startswith("{"):
                continue
            started = True
            body.append(re.sub(r"\s+#.*$", "", line) if not line.strip().startswith("#") else "")
            depth += line.count("{") - line.count("}")
            if depth == 0:
                break
        yield m.group(1), json.loads("\n".join(body))


def main():
    ok = True
    for path, body in parse_templates(HTTP_FILE.read_text()):
        req = urllib.request.Request(f"{ES}/{path}", data=json.dumps(body).encode(),
                                     method="PUT", headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                print(f"PUT {path} -> {json.loads(r.read())}")
        except Exception as e:  # show the ES error body if there is one
            ok = False
            detail = e.read().decode() if hasattr(e, "read") else e
            print(f"PUT {path} -> FAILED: {detail}", file=sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
