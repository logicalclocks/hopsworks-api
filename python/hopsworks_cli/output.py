"""Output formatting utilities for CLI"""

import json
from typing import Any, Dict, List, Union

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

from hopsworks_common import util


class OutputFormatter:
    """Format CLI output in different formats"""

    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None

    def format(self, data: Any, format_type: str = "table") -> str:
        """
        Format data according to specified format type

        # Arguments
            data: Data to format (dict, list, or object)
            format_type: Output format (json, yaml, table)

        # Returns
            Formatted string
        """
        if format_type == "json":
            return self._format_json(data)
        elif format_type == "yaml":
            return self._format_yaml(data)
        elif format_type == "table":
            return self._format_table(data)
        else:
            return str(data)

    def _format_json(self, data: Any) -> str:
        """Format as JSON"""
        return json.dumps(data, indent=2, cls=util.Encoder)

    def _format_yaml(self, data: Any) -> str:
        """Format as YAML"""
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML is required for YAML output. Install with: pip install pyyaml")
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    def _format_table(self, data: Any) -> str:
        """Format as table"""
        if not data:
            return "No data to display"

        # Convert single dict to list
        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            return str(data)

        if not data:
            return "No results"

        if RICH_AVAILABLE:
            return self._format_rich_table(data)
        else:
            return self._format_simple_table(data)

    def _format_rich_table(self, data: List[Dict]) -> str:
        """Format table using rich library"""
        if not data:
            return "No results"

        # Get headers from first item
        headers = list(data[0].keys())

        table = Table(show_header=True, header_style="bold cyan")
        for header in headers:
            table.add_column(header.replace("_", " ").title())

        for item in data:
            row = []
            for header in headers:
                value = item.get(header, "")
                # Handle None and boolean values
                if value is None:
                    value = ""
                elif isinstance(value, bool):
                    value = "✓" if value else "✗"
                elif isinstance(value, (list, dict)):
                    value = json.dumps(value, cls=util.Encoder)
                row.append(str(value))
            table.add_row(*row)

        # Capture table output as string
        from io import StringIO
        string_io = StringIO()
        console = Console(file=string_io)
        console.print(table)
        return string_io.getvalue()

    def _format_simple_table(self, data: List[Dict]) -> str:
        """Format table using simple ASCII formatting"""
        if not data:
            return "No results"

        headers = list(data[0].keys())
        col_widths = {}

        # Calculate column widths
        for header in headers:
            col_widths[header] = len(header)
            for item in data:
                value = str(item.get(header, ""))
                col_widths[header] = max(col_widths[header], len(value))

        # Create separator
        separator = "+" + "+".join("-" * (col_widths[h] + 2) for h in headers) + "+"

        # Create header row
        header_row = "|" + "|".join(f" {h:<{col_widths[h]}} " for h in headers) + "|"

        # Create data rows
        rows = []
        for item in data:
            row = "|"
            for header in headers:
                value = item.get(header, "")
                if value is None:
                    value = ""
                elif isinstance(value, bool):
                    value = "Yes" if value else "No"
                elif isinstance(value, (list, dict)):
                    value = json.dumps(value, cls=util.Encoder)
                row += f" {str(value):<{col_widths[header]}} |"
            rows.append(row)

        # Combine all parts
        result = [separator, header_row, separator]
        result.extend(rows)
        result.append(separator)

        return "\n".join(result)


def print_success(message: str):
    """Print success message in green"""
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"✓ {message}", style="bold green")
    else:
        print(f"✓ {message}")


def print_error(message: str):
    """Print error message in red"""
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"✗ {message}", style="bold red")
    else:
        print(f"✗ {message}")


def print_warning(message: str):
    """Print warning message in yellow"""
    if RICH_AVAILABLE:
        console = Console()
        console.print(f"⚠ {message}", style="bold yellow")
    else:
        print(f"⚠ {message}")
