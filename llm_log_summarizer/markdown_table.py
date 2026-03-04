from __future__ import annotations

from typing import Dict, List


def parse_markdown_table(markdown: str) -> List[Dict[str, str]]:
    """
    Parse a markdown pipe-table into a list of row dictionaries.

    Expected format:
    | col_a | col_b |
    | --- | --- |
    | v1 | v2 |
    """
    if not markdown or not markdown.strip():
        return []

    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    table_lines = [line for line in lines if "|" in line]
    if not table_lines:
        return []

    parsed_rows = [_split_markdown_row(line) for line in table_lines]
    if not parsed_rows:
        return []

    header = parsed_rows[0]
    if not header or all(not cell for cell in header):
        return []

    data_rows: List[Dict[str, str]] = []
    for cells in parsed_rows[1:]:
        if _is_separator_row(cells):
            continue

        if len(cells) < len(header):
            cells = cells + [""] * (len(header) - len(cells))
        elif len(cells) > len(header):
            cells = cells[: len(header)]

        row = {header[i]: cells[i] for i in range(len(header))}
        data_rows.append(row)

    return data_rows


def _split_markdown_row(line: str) -> List[str]:
    row = line.strip()
    if row.startswith("|"):
        row = row[1:]
    if row.endswith("|"):
        row = row[:-1]

    cells: List[str] = []
    current: List[str] = []
    escaped = False

    for ch in row:
        if escaped:
            current.append(ch)
            escaped = False
            continue

        if ch == "\\":
            escaped = True
            continue

        if ch == "|":
            cells.append("".join(current).strip())
            current = []
            continue

        current.append(ch)

    cells.append("".join(current).strip())
    return cells


def _is_separator_row(cells: List[str]) -> bool:
    if not cells:
        return False

    for cell in cells:
        token = cell.replace(" ", "")
        if not token:
            return False
        if set(token) - {":", "-"}:
            return False
        if "-" not in token:
            return False
    return True

