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

# Validation-specific formatting functions
def format_validation_quality_badge(score):
    """Format quality score badge"""
    if score >= 90:
        return "🟢 **EXCELLENT** - Data is ready for analysis"
    elif score >= 80:
        return "🟡 **GOOD** - Minor issues that should be addressed"
    elif score >= 70:
        return "🟠 **FAIR** - Several issues need attention"
    elif score >= 60:
        return "🔴 **POOR** - Significant data quality problems"
    else:
        return "⛔ **CRITICAL** - Major data quality issues require immediate attention"

def format_validation_issues(issues_list, max_issues=10):
    """Format validation issues list"""
    if not issues_list:
        return "✅ No issues found!"
    
    formatted_issues = []
    for i, issue in enumerate(issues_list[:max_issues], 1):
        if issue.startswith("HIGH"):
            formatted_issues.append(f"{i}. 🔴 {issue[6:]}")  # Remove "HIGH: " prefix
        elif issue.startswith("MEDIUM"):
            formatted_issues.append(f"{i}. 🟡 {issue[8:]}")  # Remove "MEDIUM: " prefix
        elif issue.startswith("WARNING"):
            formatted_issues.append(f"{i}. ⚠️ {issue[9:]}")  # Remove "WARNING: " prefix
        else:
            formatted_issues.append(f"{i}. ℹ️ {issue}")
    
    if len(issues_list) > max_issues:
        formatted_issues.append(f"\n... and {len(issues_list) - max_issues} more issues")
    
    return "\n".join(formatted_issues)

def format_table_summaries(validation_results):
    """Format table validation summaries"""
    summaries = []
    
    # Group results by table
    tables = {}
    for key, result in validation_results.items():
        if '_completeness' in key:
            table = key.replace('_completeness', '')
            if table not in tables:
                tables[table] = {}
            tables[table]['completeness'] = result
        elif '_types' in key:
            table = key.replace('_types', '')
            if table not in tables:
                tables[table] = {}
            tables[table]['types'] = result
        elif '_ranges' in key:
            table = key.replace('_ranges', '')
            if table not in tables:
                tables[table] = {}
            tables[table]['ranges'] = result
    
    for table, results in tables.items():
        summary = [f"### {table.upper()} Table"]
        
        if 'completeness' in results:
            comp = results['completeness']
            summary.append(f"- **Records**: {comp['total_records']:,}")
            summary.append(f"- **Columns**: {comp['total_columns']}")
            summary.append(f"- **Completeness**: {comp['completeness_score']}%")
        
        if 'types' in results:
            types = results['types']
            if types['type_issues']:
                summary.append(f"- **Type Issues**: {len(types['type_issues'])}")
        
        if 'ranges' in results:
            ranges = results['ranges']
            total_outliers = sum(outlier['count'] for outlier in ranges['outliers'].values())
            summary.append(f"- **Outliers Detected**: {total_outliers}")
        
        summaries.append("\n".join(summary))
    
    return "\n\n".join(summaries)

def format_completeness_overview(validation_results):
    """Format completeness overview"""
    overview = []
    
    for key, result in validation_results.items():
        if '_completeness' in key:
            table = key.replace('_completeness', '')
            score = result['completeness_score']
            overview.append(f"- **{table}**: {score}% complete")
    
    return "\n".join(overview)

def format_business_rules_summary(business_rules):
    """Format business rules validation summary"""
    if not business_rules:
        return "No business rules validation performed"
    
    summary = []
    summary.append(f"- **Rules Checked**: {len(business_rules.get('rules_checked', []))}")
    summary.append(f"- **Violations Found**: {len(business_rules.get('violations', []))}")
    
    if business_rules.get('violations'):
        summary.append("\n**Violations Details:**")
        for violation in business_rules['violations']:
            summary.append(f"- {violation['rule']}: {violation['violations']} violations in {violation['table']}")
    
    return "\n".join(summary)

def format_validation_recommendations(quality_score, total_issues):
    """Generate validation recommendations"""
    recommendations = []
    
    if quality_score >= 90:
        recommendations.append("✅ **Data quality is excellent** - Proceed with confidence to next pipeline stage")
        recommendations.append("✅ Consider this dataset as a quality benchmark for future ingestions")
    elif quality_score >= 80:
        recommendations.append("✅ **Data quality is good** - Address minor issues before proceeding")
        recommendations.append("⚠️ Monitor identified issues in future data loads")
    elif quality_score >= 70:
        recommendations.append("⚠️ **Data quality needs attention** - Review and fix issues before analysis")
        recommendations.append("🔄 Consider data re-processing for critical issues")
    elif quality_score >= 60:
        recommendations.append("🔴 **Data quality is poor** - Significant cleanup required")
        recommendations.append("🔄 Recommend data source investigation and re-ingestion")
    else:
        recommendations.append("⛔ **Critical data quality issues** - DO NOT proceed with this dataset")
        recommendations.append("🔄 Immediate data source review and re-collection required")
    
    if total_issues > 0:
        recommendations.append(f"📋 Review all {total_issues} identified issues in detail")
        recommendations.append("📊 Implement data quality monitoring for ongoing ingestion")
    
    return "\n".join(recommendations)

# Data preparation formatting functions
def format_cleaning_summary(cleaning_summary):
    """Format data cleaning summary"""
    if not cleaning_summary:
        return "No cleaning summary available"
    
    summary_lines = []
    for table, stats in cleaning_summary.items():
        summary_lines.append(f"### {table.upper()} Table")
        summary_lines.append(f"- **Original Records**: {stats['original_records']:,}")
        summary_lines.append(f"- **Cleaned Records**: {stats['cleaned_records']:,}")
        summary_lines.append(f"- **Records Removed**: {stats['records_removed']:,}")
        
        if stats['records_removed'] > 0:
            removal_rate = (stats['records_removed'] / stats['original_records']) * 100
            summary_lines.append(f"- **Removal Rate**: {removal_rate:.1f}%")
        
        summary_lines.append("")  # Add spacing
    
    return "\n".join(summary_lines)

def format_eda_insights(eda_insights):
    """Format EDA insights for basic preparation (no advanced features)"""
    if not eda_insights:
        return "No EDA insights available"
    
    insights = []
    
    # Overall metrics
    insights.append("### Overall Metrics")
    insights.append(f"- **Total Customers**: {eda_insights.get('total_customers', 0):,}")
    insights.append(f"- **Total Columns**: {eda_insights.get('total_columns', 0)}")
    insights.append(f"- **Churned Customers**: {eda_insights.get('churned_customers', 0):,}")
    insights.append(f"- **Churn Rate**: {eda_insights.get('churn_rate', 0):.1f}%")
    insights.append(f"- **Retention Rate**: {eda_insights.get('retention_rate', 0):.1f}%")
    insights.append("")
    
    # Data quality metrics
    insights.append("### Data Quality")
    insights.append(f"- **Overall Completeness**: {eda_insights.get('overall_completeness', 0):.1f}%")
    insights.append(f"- **Columns with Missing Data**: {eda_insights.get('columns_with_missing', 0)}")
    insights.append("")
    
    # Basic business metrics (if available)
    if eda_insights.get('avg_billing_amount'):
        insights.append("### Basic Business Metrics")
        insights.append(f"- **Average Billing Amount**: ${eda_insights.get('avg_billing_amount', 0):,.2f}")
        if eda_insights.get('avg_monthly_fee'):
            insights.append(f"- **Average Monthly Fee**: ${eda_insights.get('avg_monthly_fee', 0):,.2f}")
    
    insights.append("")
    insights.append("### Next Steps")
    insights.append("- Feature engineering will be performed in Task 6")
    insights.append("- Advanced correlations and feature selection in Task 6")
    insights.append("- This is the clean, joined dataset ready for transformation")
    
    return "\n".join(insights)

def format_quality_issues(quality_issues):
    """Format data quality issues"""
    if not quality_issues:
        return "✅ No data quality issues identified during preparation"
    
    formatted_issues = []
    for i, issue in enumerate(quality_issues, 1):
        formatted_issues.append(f"{i}. {issue}")
    
    return "\n".join(formatted_issues)

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
