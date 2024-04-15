import sys

from typing import List


def print_filter_to_stdout(include: bool, rules: List[List[str]]) -> None:
    """Print a filter file to stdout"""

    header = [
        "# Otter filter file",
        "# =================",
        "# ",
        "# This filter file defines one or more rules for filtering tasks. Each rule ",
        "# uses one or more key-value pairs to match tasks to the rule. A task",
        "# satisfies a rule if it matches all key-value pairs. Tasks are filtered",
        "# if they match at least one rule.",
        "# ",
    ]

    filter_file: List[str] = [*header]
    mode = "include" if include else "exclude"
    filter_file.append("# whether to exclude or include filtered tasks:")
    filter_file.append(f"{'mode':<8s} {mode}")
    filter_file.append("")
    for n, rule in enumerate(rules, start=1):
        keys = set()
        filter_file.append(f"# rule {n}:")
        for item in rule:
            split_at = item.find("=")
            key, value = item[0:split_at], item[split_at + 1 :]
            filter_file.append(f"{key:<8s} {value}")
            if key in keys:
                print(f'Error parsing rule {n}: repeated key "{key}"', file=sys.stderr)
                raise SystemExit(1)
            else:
                keys.add(key)
        filter_file.append(f"# end rule {n}")
        filter_file.append("")

    for line in filter_file:
        print(line)
