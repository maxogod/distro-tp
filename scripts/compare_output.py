#!/usr/bin/env python3
import csv
import sys
from pathlib import Path


def normalize_value(value: str):
    try:
        num = float(value)
        return f"{num:.1f}" # Ignore decimals (e.g. 1.00)
    except ValueError:
        return value.strip()


def read_csv_as_set(filename):
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = {tuple(normalize_value(cell) for cell in row) for row in reader}
    return header, rows


def compare_csv_files(file1: str, file2: str) -> bool:
    header1, rows1 = read_csv_as_set(file1)
    header2, rows2 = read_csv_as_set(file2)

    print(f"Comparing '{file1}' with '{file2}'")

    if header1 != header2:
        print("FAIL! | Headers differ:\n{file1}: {header1}\n{file2}: {header2}\n")
        return False

    missing_in_file2 = rows1 - rows2
    missing_in_file1 = rows2 - rows1

    if not missing_in_file1 and not missing_in_file2:
        print("SUCCESS! | Both CSV files contain the same rows!\n")
        return True

    print("FAIL! | CSV files differ:")

    if missing_in_file2:
        print(f"\nRows present in {Path(file1).name} but missing in {Path(file2).name}:")
        for row in sorted(missing_in_file2):
            print("  ", row)

    if missing_in_file1:
        print(f"\nRows present in {Path(file2).name} but missing in {Path(file1).name}:")
        for row in sorted(missing_in_file1):
            print("  ", row)
    return False


def task4_comparison(file1: str, file2: str) -> bool:
    count1 = {}
    count2 = {}
    with open(file1, "r", encoding="utf-8") as f1, open(file2, "r", encoding="utf-8") as f2:
        print(f"Comparing '{file1}' with '{file2}'")

        next(f1)  # Skip header
        next(f2)  # Skip header
        for line in f1:
            store, _, qty = line.strip().split(",")
            count1[store] = count1.get(store, 0) + int(qty)
        for line in f2:
            store, _, qty = line.strip().split(",")
            count2[store] = count2.get(store, 0) + int(qty)

    if len(count1) != len(count2):
        print(f"FAIL! | Number of stores differ\n")
        return False
    
    for store in count1:
        if store not in count2 or count1[store] != count2[store]:
            print(f"FAIL! | Store '{store}' has different total quantities: {count1.get(store, 0)} vs {count2.get(store, 0)}\n")
            return False

    print("SUCCESS! | Both files have the same total quantities per store!\n")
    return True


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_csvs.py <file1.csv> <file2.csv>")
        sys.exit(1)

    file1, file2 = sys.argv[1], sys.argv[2]

    if not Path(file1).exists() or not Path(file2).exists():
        print("One or both files do not exist.")
        sys.exit(1)

    task = file1.split("/")[1].split(".")[0]
    if task == "t4":
        if not task4_comparison(file1, file2):
            sys.exit(1)
    else:
        if not compare_csv_files(file1, file2):
            sys.exit(1)


if __name__ == "__main__":
    main()
