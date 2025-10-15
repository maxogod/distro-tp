#!/usr/bin/env python3
import csv
import sys
from pathlib import Path


def normalize_value(value: str):
    """Try to normalize numbers by rounding to 1 decimal place, else return as string."""
    try:
        # Try converting to float and round to 1 decimal
        num = float(value)
        return f"{num:.1f}"
    except ValueError:
        # Not a number, return as is (trim whitespace)
        return value.strip()


def read_csv_as_set(filename):
    """Read a CSV file (excluding header) and return a set of normalized tuples."""
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        rows = {tuple(normalize_value(cell) for cell in row) for row in reader}
    return header, rows


def compare_csv_files(file1: str, file2: str):
    """Compare two CSV files for identical content regardless of row order,
    ignoring differences beyond the first decimal place in numeric columns.
    """
    header1, rows1 = read_csv_as_set(file1)
    header2, rows2 = read_csv_as_set(file2)

    print(f"Comparing '{file1}' with '{file2}'\n")

    if header1 != header2:
        print("FAIL! | Headers differ:")
        print(f"  {file1}: {header1}")
        print(f"  {file2}: {header2}")
        return

    missing_in_file2 = rows1 - rows2
    missing_in_file1 = rows2 - rows1

    if not missing_in_file1 and not missing_in_file2:
        print("SUCCESS! | Both CSV files contain the same rows!")
        return

    print("FAIL! | CSV files differ:")

    if missing_in_file2:
        print(
            f"\nRows present in {Path(file1).name} but missing in {Path(file2).name}:"
        )
        for row in sorted(missing_in_file2):
            print("  ", row)

    if missing_in_file1:
        print(
            f"\nRows present in {Path(file2).name} but missing in {Path(file1).name}:"
        )
        for row in sorted(missing_in_file1):
            print("  ", row)


def main():
    if len(sys.argv) != 3:
        print("Usage: python compare_csvs.py <file1.csv> <file2.csv>")
        sys.exit(1)

    file1, file2 = sys.argv[1], sys.argv[2]

    if not Path(file1).exists() or not Path(file2).exists():
        print("One or both files do not exist.")
        sys.exit(1)

    compare_csv_files(file1, file2)


if __name__ == "__main__":
    main()
