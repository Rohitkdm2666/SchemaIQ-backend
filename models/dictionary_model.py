"""
Data Dictionary Models
Pydantic models for the intelligent data dictionary system
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class DictionaryColumnModel(BaseModel):
    """Enhanced column model with AI-generated context"""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Data type")
    nullable: bool = Field(default=True, description="Whether column allows NULL values")
    primary_key: bool = Field(default=False, description="Whether column is part of primary key")
    foreign_key: Optional[str] = Field(default=None, description="Referenced table if foreign key")
    
    # Data profiling information
    sample_values: List[str] = Field(default=[], description="Sample data values for context")
    null_percentage: float = Field(default=0.0, description="Percentage of NULL values")
    unique_count: int = Field(default=0, description="Number of unique values")
    unique_percentage: float = Field(default=0.0, description="Percentage of unique values")
    min_length: Optional[int] = Field(default=None, description="Minimum value length")
    max_length: Optional[int] = Field(default=None, description="Maximum value length")
    avg_length: Optional[float] = Field(default=None, description="Average value length")
    
    # AI-generated content
    description: str = Field(default="Auto-extracted column", description="Technical description")
    business_context: str = Field(default="Pending AI context", description="Business meaning and purpose")
    inferred_content_type: str = Field(default="General data", description="Inferred semantic content type")
    data_patterns: List[str] = Field(default=[], description="Detected data patterns")
    quality_score: float = Field(default=0.0, description="Data quality score (0-1)")
    
    # Metadata
    domain_category: str = Field(default="general", description="Business domain category")
    confidence: float = Field(default=0.0, description="AI confidence score (0-1)")

class DictionaryTableModel(BaseModel):
    """Enhanced table model with comprehensive context"""
    name: str = Field(..., description="Table name")
    row_count: int = Field(default=0, description="Total number of rows")
    column_count: int = Field(default=0, description="Total number of columns")
    
    # Schema information
    primary_keys: List[str] = Field(default=[], description="Primary key columns")
    foreign_keys: List[Dict[str, Any]] = Field(default=[], description="Foreign key relationships")
    
    # AI-generated content
    description: str = Field(default="Database table", description="Technical description")
    business_purpose: str = Field(default="Data storage and management", description="Business purpose")
    domain_role: str = Field(default="Supporting entity", description="Role in business domain")
    summary: str = Field(default="Dynamically loaded from database schema", description="AI summary")
    
    # Data quality metrics
    data_density: float = Field(default=0.0, description="Data density score (0-1)")
    completeness_score: float = Field(default=0.0, description="Data completeness score (0-1)")
    consistency_score: float = Field(default=0.0, description="Data consistency score (0-1)")
    
    # Relationships
    relationships_summary: str = Field(default="No relationships defined", description="Summary of table relationships")
    
    # Metadata
    data_category: str = Field(default="operational_data", description="Table data category")
    domain: str = Field(default="general", description="Business domain")
    
    # Columns
    columns: List[DictionaryColumnModel] = Field(default=[], description="Column definitions with context")

class DomainAnalysisModel(BaseModel):
    """Domain classification results"""
    primary_domain: str = Field(..., description="Primary business domain")
    confidence: float = Field(..., description="Classification confidence (0-1)")
    secondary_domains: List[Dict[str, float]] = Field(default=[], description="Secondary domains with scores")
    business_type: str = Field(..., description="Business model type")
    industry_vertical: str = Field(..., description="Industry vertical")
    complexity_score: float = Field(..., description="Schema complexity score (0-1)")
    
    # Domain context
    description: str = Field(default="", description="Domain description")
    common_entities: List[str] = Field(default=[], description="Common entities in this domain")

class BusinessWorkflowModel(BaseModel):
    """Business workflow information"""
    workflow_name: str = Field(..., description="Workflow name")
    description: str = Field(..., description="Workflow description")
    involved_tables: List[str] = Field(default=[], description="Tables involved in workflow")
    process_flow: str = Field(default="", description="Process flow description")

class DataQualitySummaryModel(BaseModel):
    """Data quality summary"""
    overall_quality: float = Field(..., description="Overall quality score (0-1)")
    completeness: float = Field(..., description="Data completeness score")
    consistency: float = Field(..., description="Data consistency score")
    data_density: float = Field(..., description="Data density score")
    
    # Quality distribution
    high_quality_tables: int = Field(default=0, description="Number of high-quality tables")
    medium_quality_tables: int = Field(default=0, description="Number of medium-quality tables")
    low_quality_tables: int = Field(default=0, description="Number of low-quality tables")
    
    # Issues and recommendations
    quality_issues: List[str] = Field(default=[], description="Identified quality issues")
    recommendations: List[str] = Field(default=[], description="Quality improvement recommendations")

class DictionaryResponse(BaseModel):
    """Complete data dictionary response"""
    # Schema overview
    schema_name: str = Field(default="Database Schema", description="Schema name")
    total_tables: int = Field(..., description="Total number of tables")
    total_columns: int = Field(..., description="Total number of columns")
    total_rows: int = Field(default=0, description="Total number of rows across all tables")
    
    # Domain analysis
    domain_analysis: DomainAnalysisModel = Field(..., description="Domain classification results")
    
    # Business context
    business_summary: str = Field(..., description="AI-generated business summary")
    technical_summary: str = Field(..., description="Technical architecture summary")
    
    # Data quality
    quality_summary: DataQualitySummaryModel = Field(..., description="Data quality assessment")
    
    # Workflows
    business_workflows: List[BusinessWorkflowModel] = Field(default=[], description="Identified business workflows")
    
    # Tables and columns
    tables: List[DictionaryTableModel] = Field(..., description="Table definitions with AI context")
    
    # Metadata
    generated_at: str = Field(..., description="Generation timestamp")
    ai_engine: str = Field(default="SchemaIQ Local AI Agent", description="AI engine used")
    confidence_score: float = Field(..., description="Overall AI confidence (0-1)")
    
    # Export information
    export_formats: List[str] = Field(default=["markdown", "csv", "json", "pdf"], description="Available export formats")

class ExportRequest(BaseModel):
    """Export request model"""
    format: str = Field(..., description="Export format (markdown, csv, json, pdf)")
    include_samples: bool = Field(default=True, description="Include sample data")
    include_quality_metrics: bool = Field(default=True, description="Include quality metrics")
    include_business_context: bool = Field(default=True, description="Include business context")
    tables: Optional[List[str]] = Field(default=None, description="Specific tables to export (None for all)")

class DictionaryUpdateRequest(BaseModel):
    """Request to update dictionary content"""
    table_name: str = Field(..., description="Table name")
    column_name: Optional[str] = Field(default=None, description="Column name (None for table-level update)")
    description: Optional[str] = Field(default=None, description="Updated description")
    business_context: Optional[str] = Field(default=None, description="Updated business context")
    tags: Optional[List[str]] = Field(default=None, description="Custom tags")

class DictionarySearchRequest(BaseModel):
    """Search request for dictionary content"""
    query: str = Field(..., description="Search query")
    search_in: List[str] = Field(default=["names", "descriptions", "business_context"], 
                                description="Fields to search in")
    domain_filter: Optional[str] = Field(default=None, description="Filter by domain")
    table_filter: Optional[str] = Field(default=None, description="Filter by table")
    include_columns: bool = Field(default=True, description="Include column-level results")

class DictionarySearchResult(BaseModel):
    """Search result item"""
    type: str = Field(..., description="Result type (table or column)")
    table_name: str = Field(..., description="Table name")
    column_name: Optional[str] = Field(default=None, description="Column name if applicable")
    name: str = Field(..., description="Item name")
    description: str = Field(..., description="Description")
    business_context: str = Field(..., description="Business context")
    relevance_score: float = Field(..., description="Search relevance score (0-1)")
    match_highlights: List[str] = Field(default=[], description="Highlighted matching text")

class DictionarySearchResponse(BaseModel):
    """Search response"""
    query: str = Field(..., description="Original search query")
    total_results: int = Field(..., description="Total number of results")
    results: List[DictionarySearchResult] = Field(..., description="Search results")
    search_time_ms: float = Field(..., description="Search execution time in milliseconds")
    suggestions: List[str] = Field(default=[], description="Search suggestions")
