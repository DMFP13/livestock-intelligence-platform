from __future__ import annotations

import argparse
import json

from apps.api.service import PlatformService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ingestion pipeline")
    parser.add_argument("--connector", required=True)
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--mode", default="uploaded_file")
    parser.add_argument("--config-json", default="{}", help="JSON string for connector config")
    args = parser.parse_args()

    service = PlatformService()
    result = service.run_ingestion(
        connector_key=args.connector,
        source_system=args.source_system,
        mode=args.mode,
        config=json.loads(args.config_json),
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
