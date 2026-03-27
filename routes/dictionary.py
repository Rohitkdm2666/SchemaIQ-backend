"""
Data Dictionary API Endpoints
Intelligent data dictionary generation and management
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
import traceback
from datetime import datetime

from db.connection import get_engine
from services.ai_agent.local_ai_agent import SchemaIQLocalAI
from models.dictionary_model import (
    DictionaryResponse, ExportRequest, DictionaryUpdateRequest,
    DictionarySearchRequest, DictionarySearchResponse
)

router = APIRouter(prefix="/dictionary", tags=["Data Dictionary"])

# Initialize the local AI agent
local_ai = SchemaIQLocalAI()


def _empty_dictionary_response(message: str) -> DictionaryResponse:
    return DictionaryResponse(
        schema_name="Database Schema",
        total_tables=0,
        total_columns=0,
        total_rows=0,
        domain_analysis={
            "primary_domain": "unknown",
            "confidence": 0.0,
            "secondary_domains": [],
            "business_type": "Unknown",
            "industry_vertical": "Unknown",
            "complexity_score": 0.0,
            "description": "",
            "common_entities": [],
        },
        business_summary=message,
        technical_summary="No schema data available.",
        quality_summary={
            "overall_quality": 0.0,
            "completeness": 0.0,
            "consistency": 0.0,
            "data_density": 0.0,
            "high_quality_tables": 0,
            "medium_quality_tables": 0,
            "low_quality_tables": 0,
            "quality_issues": [],
            "recommendations": [],
        },
        business_workflows=[],
        tables=[],
        generated_at=datetime.now().isoformat(),
        ai_engine="SchemaIQ Local AI Agent v1.0",
        confidence_score=0.0,
    )

@router.get("/", response_model=DictionaryResponse)
async def get_intelligent_dictionary(
    db_url: Optional[str] = Query(None, description="Database URL (uses active connection if not provided)"),
    include_profiling: bool = Query(True, description="Include data profiling analysis"),
    sample_size: int = Query(1000, description="Sample size for data profiling"),
    tables: Optional[str] = Query(None, description="Comma-separated list of specific tables to analyze")
):
    """
    Generate comprehensive intelligent data dictionary using local AI agent
    
    This endpoint creates a complete data dictionary with:
    - AI-generated business context and descriptions
    - Domain classification and industry analysis
    - Data quality assessment and profiling
    - Relationship mapping and workflow identification
    - Pattern recognition and content type inference
    
    All processing is done locally without external API dependencies.
    """
    try:
        # Get database engine
        engine = get_engine(db_url)
        if not engine:
            raise HTTPException(
                status_code=400, 
                detail="No database connection available. Please connect to a database first."
            )
        
        # Validate sample size
        if sample_size < 10 or sample_size > 10000:
            raise HTTPException(
                status_code=400,
                detail="Sample size must be between 10 and 10000"
            )
        
        # Parse specific tables if provided
        specific_tables = None
        if tables:
            specific_tables = [t.strip() for t in tables.split(',') if t.strip()]
        
        # Generate intelligent dictionary
        dictionary = local_ai.generate_intelligent_dictionary(
            engine=engine,
            include_profiling=include_profiling,
            sample_size=sample_size
        )
        
        # Filter tables if specific ones requested
        if specific_tables:
            filtered_tables = [
                table for table in dictionary.tables 
                if table.name in specific_tables
            ]
            dictionary.tables = filtered_tables
            dictionary.total_tables = len(filtered_tables)
            dictionary.total_columns = sum(table.column_count for table in filtered_tables)
        
        return dictionary
        
    except HTTPException:
        raise
    except Exception as e:
        error_detail = f"Failed to generate dictionary: {str(e)}"
        if "connection" in str(e).lower():
            error_detail = "Database connection error. Please verify your connection and try again."
        elif "permission" in str(e).lower():
            error_detail = "Database permission error. Ensure you have read access to the schema."
        
        raise HTTPException(status_code=500, detail=error_detail)

@router.get("/quick", response_model=DictionaryResponse)
async def get_quick_dictionary(
    db_url: Optional[str] = Query(None, description="Database URL (uses active connection if not provided)"),
    tables: Optional[str] = Query(None, description="Comma-separated list of specific tables")
):
    """
    Generate quick data dictionary without extensive profiling
    
    Faster version that focuses on schema analysis and AI context generation
    without detailed data profiling. Suitable for large databases or quick analysis.
    """
    try:
        engine = get_engine(db_url)
        if not engine:
            return _empty_dictionary_response("No database connection available.")
        
        # Generate dictionary without profiling for speed
        dictionary = local_ai.generate_intelligent_dictionary(
            engine=engine,
            include_profiling=False,
            sample_size=100  # Minimal sampling
        )
        
        # Filter tables if requested
        if tables:
            specific_tables = [t.strip() for t in tables.split(',') if t.strip()]
            filtered_tables = [
                table for table in dictionary.tables 
                if table.name in specific_tables
            ]
            dictionary.tables = filtered_tables
            dictionary.total_tables = len(filtered_tables)
        
        return dictionary
        
    except HTTPException as e:
        return _empty_dictionary_response(str(e.detail))
    except Exception as e:
        return _empty_dictionary_response(f"Failed to generate quick dictionary: {str(e)}")

@router.get("/domain-analysis")
async def get_domain_analysis(
    db_url: Optional[str] = Query(None, description="Database URL")
):
    """
    Get detailed domain classification and business analysis
    
    Returns comprehensive domain analysis including:
    - Primary and secondary business domains
    - Industry vertical classification
    - Business type identification
    - Complexity assessment
    """
    try:
        engine = get_engine(db_url)
        if not engine:
            raise HTTPException(status_code=400, detail="No database connection available")
        
        # Generate minimal dictionary to get domain analysis
        dictionary = local_ai.generate_intelligent_dictionary(
            engine=engine,
            include_profiling=False,
            sample_size=50
        )
        
        return {
            "domain_analysis": dictionary.domain_analysis,
            "business_summary": dictionary.business_summary,
            "business_workflows": dictionary.business_workflows,
            "generated_at": dictionary.generated_at
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze domain: {str(e)}"
        )

@router.get("/quality-report")
async def get_quality_report(
    db_url: Optional[str] = Query(None, description="Database URL"),
    sample_size: int = Query(1000, description="Sample size for analysis")
):
    """
    Get comprehensive data quality assessment
    
    Returns detailed quality metrics including:
    - Completeness and consistency scores
    - Data density analysis
    - Quality issues and recommendations
    - Column-level quality metrics
    """
    try:
        engine = get_engine(db_url)
        if not engine:
            raise HTTPException(status_code=400, detail="No database connection available")
        
        # Generate dictionary with focus on profiling
        dictionary = local_ai.generate_intelligent_dictionary(
            engine=engine,
            include_profiling=True,
            sample_size=sample_size
        )
        
        # Extract quality information
        quality_data = {
            "quality_summary": dictionary.quality_summary,
            "technical_summary": dictionary.technical_summary,
            "table_quality": []
        }
        
        # Add table-level quality details
        for table in dictionary.tables:
            table_quality = {
                "table_name": table.name,
                "row_count": table.row_count,
                "completeness_score": table.completeness_score,
                "consistency_score": table.consistency_score,
                "data_density": table.data_density,
                "column_quality": [
                    {
                        "column_name": col.name,
                        "quality_score": col.quality_score,
                        "null_percentage": col.null_percentage,
                        "unique_percentage": col.unique_percentage,
                        "data_patterns": col.data_patterns,
                        "inferred_content_type": col.inferred_content_type
                    }
                    for col in table.columns
                ]
            }
            quality_data["table_quality"].append(table_quality)
        
        return quality_data
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate quality report: {str(e)}"
        )

@router.get("/export/{format}")
async def export_dictionary(
    format: str,
    db_url: Optional[str] = Query(None, description="Database URL"),
    include_samples: bool = Query(True, description="Include sample data"),
    include_quality: bool = Query(True, description="Include quality metrics"),
    include_business: bool = Query(True, description="Include business context"),
    tables: Optional[str] = Query(None, description="Specific tables to export")
):
    """
    Export data dictionary in various formats
    
    Supported formats:
    - markdown: Human-readable Markdown documentation
    - csv: Tabular CSV format for spreadsheet analysis
    - json: Structured JSON for programmatic use
    - html: Web-ready HTML documentation
    """
    try:
        if format not in ["markdown", "csv", "json", "html"]:
            raise HTTPException(
                status_code=400,
                detail="Unsupported format. Use: markdown, csv, json, or html"
            )
        
        engine = get_engine(db_url)
        if not engine:
            raise HTTPException(status_code=400, detail="No database connection available")
        
        # Generate dictionary
        dictionary = local_ai.generate_intelligent_dictionary(
            engine=engine,
            include_profiling=include_quality,
            sample_size=500 if include_samples else 100
        )
        
        # Filter tables if specified
        if tables:
            specific_tables = [t.strip() for t in tables.split(',') if t.strip()]
            dictionary.tables = [
                table for table in dictionary.tables 
                if table.name in specific_tables
            ]
        
        # Generate export content based on format
        if format == "json":
            return dictionary.dict()
        elif format == "markdown":
            return {"content": _generate_markdown_export(dictionary, include_samples, include_quality, include_business)}
        elif format == "csv":
            return {"content": _generate_csv_export(dictionary, include_samples, include_quality)}
        elif format == "html":
            return {"content": _generate_html_export(dictionary, include_samples, include_quality, include_business)}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export dictionary: {str(e)}"
        )

@router.post("/search", response_model=DictionarySearchResponse)
async def search_dictionary(
    search_request: DictionarySearchRequest,
    db_url: Optional[str] = Query(None, description="Database URL")
):
    """
    Search through data dictionary content
    
    Intelligent search across:
    - Table and column names
    - Descriptions and business context
    - Data patterns and content types
    - Domain classifications
    """
    try:
        # This would implement intelligent search functionality
        # For now, return a placeholder response
        return DictionarySearchResponse(
            query=search_request.query,
            total_results=0,
            results=[],
            search_time_ms=0.0,
            suggestions=[]
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(e)}"
        )

def _generate_markdown_export(dictionary: DictionaryResponse, 
                            include_samples: bool, include_quality: bool, 
                            include_business: bool) -> str:
    """Generate Markdown export format"""
    
    md_content = f"""# Data Dictionary - {dictionary.schema_name}

**Generated:** {dictionary.generated_at}  
**AI Engine:** {dictionary.ai_engine}  
**Confidence:** {dictionary.confidence_score:.1%}

## Overview

- **Total Tables:** {dictionary.total_tables}
- **Total Columns:** {dictionary.total_columns}
- **Total Rows:** {dictionary.total_rows:,}
- **Primary Domain:** {dictionary.domain_analysis.primary_domain.title()}
- **Business Type:** {dictionary.domain_analysis.business_type}
- **Industry:** {dictionary.domain_analysis.industry_vertical}

## Business Summary

{dictionary.business_summary}

## Technical Summary

{dictionary.technical_summary}
"""

    if include_quality:
        md_content += f"""
## Data Quality Assessment

- **Overall Quality:** {dictionary.quality_summary.overall_quality:.1%}
- **Completeness:** {dictionary.quality_summary.completeness:.1%}
- **Consistency:** {dictionary.quality_summary.consistency:.1%}
- **Data Density:** {dictionary.quality_summary.data_density:.1%}

### Quality Distribution
- High Quality Tables: {dictionary.quality_summary.high_quality_tables}
- Medium Quality Tables: {dictionary.quality_summary.medium_quality_tables}
- Low Quality Tables: {dictionary.quality_summary.low_quality_tables}
"""

    md_content += "\n## Tables\n"
    
    for table in dictionary.tables:
        md_content += f"\n### {table.name}\n\n"
        md_content += f"**Description:** {table.description}\n\n"
        
        if include_business:
            md_content += f"**Business Purpose:** {table.business_purpose}\n\n"
            md_content += f"**Domain Role:** {table.domain_role}\n\n"
        
        md_content += f"- **Rows:** {table.row_count:,}\n"
        md_content += f"- **Columns:** {table.column_count}\n"
        
        if include_quality:
            md_content += f"- **Completeness:** {table.completeness_score:.1%}\n"
            md_content += f"- **Consistency:** {table.consistency_score:.1%}\n"
        
        if table.primary_keys:
            md_content += f"- **Primary Keys:** {', '.join(table.primary_keys)}\n"
        
        md_content += "\n#### Columns\n\n"
        md_content += "| Column | Type | Description | Business Context |\n"
        md_content += "|--------|------|-------------|------------------|\n"
        
        for column in table.columns:
            desc = column.description.replace('\n', ' ')
            business = column.business_context.replace('\n', ' ') if include_business else ""
            md_content += f"| {column.name} | {column.type} | {desc} | {business} |\n"
    
    return md_content

def _generate_csv_export(dictionary: DictionaryResponse, 
                        include_samples: bool, include_quality: bool) -> str:
    """Generate CSV export format"""
    
    csv_lines = ["Table,Column,Type,Primary Key,Foreign Key,Description,Business Context,Null %,Unique Count,Quality Score"]
    
    for table in dictionary.tables:
        for column in table.columns:
            row = [
                table.name,
                column.name,
                column.type,
                "Yes" if column.primary_key else "No",
                column.foreign_key or "",
                f'"{column.description}"',
                f'"{column.business_context}"',
                f"{column.null_percentage:.1f}%" if include_quality else "",
                str(column.unique_count) if include_quality else "",
                f"{column.quality_score:.2f}" if include_quality else ""
            ]
            csv_lines.append(",".join(row))
    
    return "\n".join(csv_lines)

def _generate_html_export(dictionary: DictionaryResponse, 
                         include_samples: bool, include_quality: bool, 
                         include_business: bool) -> str:
    """Generate HTML export format"""
    
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Dictionary - {dictionary.schema_name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
        .table-section {{ margin: 20px 0; }}
        .column-table {{ width: 100%; border-collapse: collapse; }}
        .column-table th, .column-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .column-table th {{ background-color: #f2f2f2; }}
        .quality-high {{ color: green; }}
        .quality-medium {{ color: orange; }}
        .quality-low {{ color: red; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Dictionary - {dictionary.schema_name}</h1>
        <p><strong>Generated:</strong> {dictionary.generated_at}</p>
        <p><strong>AI Engine:</strong> {dictionary.ai_engine}</p>
        <p><strong>Confidence:</strong> {dictionary.confidence_score:.1%}</p>
    </div>
    
    <h2>Overview</h2>
    <ul>
        <li><strong>Total Tables:</strong> {dictionary.total_tables}</li>
        <li><strong>Total Columns:</strong> {dictionary.total_columns}</li>
        <li><strong>Total Rows:</strong> {dictionary.total_rows:,}</li>
        <li><strong>Primary Domain:</strong> {dictionary.domain_analysis.primary_domain.title()}</li>
        <li><strong>Business Type:</strong> {dictionary.domain_analysis.business_type}</li>
    </ul>
    
    <h2>Business Summary</h2>
    <p>{dictionary.business_summary}</p>
"""

    for table in dictionary.tables:
        quality_class = "quality-high" if table.completeness_score > 0.8 else "quality-medium" if table.completeness_score > 0.6 else "quality-low"
        
        html_content += f"""
    <div class="table-section">
        <h3>{table.name}</h3>
        <p><strong>Description:</strong> {table.description}</p>
        {f'<p><strong>Business Purpose:</strong> {table.business_purpose}</p>' if include_business else ''}
        <p><strong>Rows:</strong> {table.row_count:,} | <strong>Columns:</strong> {table.column_count}</p>
        {f'<p><strong>Quality:</strong> <span class="{quality_class}">{table.completeness_score:.1%}</span></p>' if include_quality else ''}
        
        <table class="column-table">
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Description</th>
                    {f'<th>Business Context</th>' if include_business else ''}
                    {f'<th>Quality</th>' if include_quality else ''}
                </tr>
            </thead>
            <tbody>
"""
        
        for column in table.columns:
            col_quality_class = "quality-high" if column.quality_score > 0.7 else "quality-medium" if column.quality_score > 0.5 else "quality-low"
            
            html_content += f"""
                <tr>
                    <td><strong>{column.name}</strong>{' (PK)' if column.primary_key else ''}</td>
                    <td>{column.type}</td>
                    <td>{column.description}</td>
                    {f'<td>{column.business_context}</td>' if include_business else ''}
                    {f'<td><span class="{col_quality_class}">{column.quality_score:.2f}</span></td>' if include_quality else ''}
                </tr>
"""
        
        html_content += """
            </tbody>
        </table>
    </div>
"""
    
    html_content += """
</body>
</html>
"""
    
    return html_content
