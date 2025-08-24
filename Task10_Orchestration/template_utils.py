"""
Template utilities for Prefect artifacts
Handles markdown template loading and variable substitution
"""

import os
from string import Template
from datetime import datetime

def load_markdown_template(template_name, **kwargs):
    """
    Load a markdown template and substitute variables
    
    Args:
        template_name: Name of the template file (without .md extension)
        **kwargs: Variables to substitute in the template
        
    Returns:
        Formatted markdown string
    """
    template_path = os.path.join(
        os.path.dirname(__file__), 
        "templates", 
        f"{template_name}.md"
    )
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        
        # Use Python's string formatting for variable substitution
        return template_content.format(**kwargs)
        
    except FileNotFoundError:
        return f"Template not found: {template_name}.md"
    except KeyError as e:
        return f"Missing template variable: {e}"
    except Exception as e:
        return f"Template error: {e}"

def format_table_details(data):
    """Helper function to format table details for templates"""
    details = []
    for source in data:
        details.append(f"- **{source['table']}**: {source.get('records', 0)} records")
    return "\n".join(details)

def format_file_details(storage_results):
    """Helper function to format file details for templates"""
    if not storage_results:
        return "No file details available"
    
    details = []
    for table, result in storage_results.items():
        # file_path = result.get('file_path', 'N/A')
        # records = result.get('records_stored', 0)
        details.append(f"- **{table}.csv**: → `{result}`")
    
    return "\n".join(details)

def format_storage_tree(tables, date):
    """Helper function to create storage tree structure"""
    tree_lines = []
    for i, table in enumerate(tables):
        is_last = (i == len(tables) - 1)
        prefix = "└──" if is_last else "├──"
        tree_lines.append(f"{prefix} {table}/")
        tree_lines.append(f"{'    ' if is_last else '│   '}└── dt={date}/")
        tree_lines.append(f"{'        ' if is_last else '│       '}└── {table}.csv")
    
    return "\n".join(tree_lines)

# Example usage:
if __name__ == "__main__":
    # Test template loading
    test_data = {
        "total_records": 100,
        "tables_count": 2,
        "ingestion_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "table_details": "- billing: 50 records\n- subscriptions: 50 records",
        "execution_time": "2.5 seconds",
        "avg_records_per_table": 50
    }
    
    result = load_markdown_template("ingestion_report_template", **test_data)
    print(result)
