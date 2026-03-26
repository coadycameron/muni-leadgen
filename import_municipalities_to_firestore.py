#!/usr/bin/env python3

import argparse
from muni_leadgen.firestore_store import FirestoreMunicipalityStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--xlsx", required=True, help="Path to named_places_population_usa.xlsx")
    parser.add_argument("--sheet-name", default=None, help="Optional workbook sheet name")
    args = parser.parse_args()

    store = FirestoreMunicipalityStore()
    stats = store.import_master_list_from_xlsx(args.xlsx, sheet_name=args.sheet_name)
    print(stats)


if __name__ == "__main__":
    main()
