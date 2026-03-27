"""
SchemaIQ Local AI Agent - Main Integration Module
Universal database intelligence without external dependencies
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json
from sqlalchemy import Engine

from .universal_patterns import UniversalPatternEngine
from .domain_classifier import UniversalDomainClassifier
from .context_generator import IntelligentContextGenerator
from .data_profiler import IntelligentDataProfiler
from services.schema_extractor.extractor import extract_schema
from services.relationship_mapper.fk_mapper import map_foreign_keys
from services.relationship_mapper.infer_mapper import infer_relationships
from db.inspector import inspect_database
from models.dictionary_model import (
    DictionaryResponse, DictionaryTableModel, DictionaryColumnModel,
    DomainAnalysisModel, DataQualitySummaryModel, BusinessWorkflowModel
)

class SchemaIQLocalAI:
    """
    Main Local AI Agent for comprehensive database intelligence
    """
    
    def __init__(self):
        self.pattern_engine = UniversalPatternEngine()
        self.domain_classifier = UniversalDomainClassifier()
        self.context_generator = IntelligentContextGenerator()
        self.data_profiler = IntelligentDataProfiler()
        self._current_engine = None  # Store current engine for row counting
        
    def generate_intelligent_dictionary(self, engine: Engine, 
                                      include_profiling: bool = True,
                                      sample_size: int = 1000) -> DictionaryResponse:
        """
        Generate comprehensive intelligent data dictionary
        """
        try:
            # Store engine for row counting
            self._current_engine = engine
            
            # Step 1: Extract and structure schema
            raw_schema = inspect_database(engine)
            structured_schema = extract_schema(raw_schema)
            
            # Step 2: Map relationships
            schema_with_fks = map_foreign_keys(structured_schema)
            relationships = schema_with_fks.get('relationships', [])
            
            # Step 3: Infer additional relationships
            inferred_rels = infer_relationships(schema_with_fks)
            # Fix: inferred_rels returns a dict, not a list
            if isinstance(inferred_rels, dict):
                all_relationships = relationships + inferred_rels.get('relationships', [])
            else:
                # If inferred_rels is already a list of relationships
                all_relationships = relationships + (inferred_rels if isinstance(inferred_rels, list) else [])
            
            # Step 4: Domain classification
            tables_list = structured_schema.get('tables', [])
            
            # Debug: Check if tables_list is actually a list
            if not isinstance(tables_list, list):
                print(f"ERROR: tables_list is not a list, it's {type(tables_list)}: {tables_list}")
                return self._build_error_response(f"Invalid schema structure: tables is {type(tables_list)}")
            
            domain_analysis = self.domain_classifier.classify_schema(tables_list, all_relationships)
            
            # Step 5: Data profiling (if enabled)
            table_profiles = {}
            profiling_summary = {}
            
            if include_profiling:
                for table in tables_list:
                    table_name = table.get('name', '')
                    try:
                        profile = self.data_profiler.profile_table(engine, table_name, sample_size)
                        table_profiles[table_name] = profile
                    except Exception as e:
                        # Continue without profiling for problematic tables
                        continue
                
                if table_profiles:
                    profiling_summary = self.data_profiler.get_profiling_summary(list(table_profiles.values()))
            
            # Step 6: Generate comprehensive context
            sample_data = self._extract_sample_data_from_profiles(table_profiles)
            schema_context = self.context_generator.generate_comprehensive_context(
                tables_list, all_relationships, sample_data
            )
            
            # Step 7: Build response models
            dictionary_tables = []
            total_rows = 0
            
            for i, table in enumerate(tables_list):
                table_name = table.get('name', '')
                table_context = schema_context.tables[i] if i < len(schema_context.tables) else None
                table_profile = table_profiles.get(table_name)
                
                # Build column models
                dictionary_columns = []
                table_column_contexts = schema_context.columns.get(table_name, [])
                
                for j, column in enumerate(table.get('columns', [])):
                    column_context = table_column_contexts[j] if j < len(table_column_contexts) else None
                    column_profile = None
                    
                    if table_profile:
                        column_profile = next(
                            (cp for cp in table_profile.columns if cp.column_name == column.get('name')),
                            None
                        )
                    
                    dict_column = self._build_dictionary_column(column, column_context, column_profile)
                    dictionary_columns.append(dict_column)
                
                # Build table model
                dict_table = self._build_dictionary_table(
                    table, table_context, table_profile, dictionary_columns, all_relationships
                )
                dictionary_tables.append(dict_table)
                
                if table_profile:
                    total_rows += table_profile.row_count
            
            # Step 8: Build domain analysis model
            domain_model = self._build_domain_analysis_model(domain_analysis)
            
            # Step 9: Build quality summary
            quality_summary = self._build_quality_summary(profiling_summary, table_profiles)
            
            # Step 10: Build business workflows
            workflows = self._build_business_workflows(domain_analysis, tables_list, all_relationships)
            
            # Step 11: Calculate overall confidence
            overall_confidence = self._calculate_overall_confidence(
                domain_analysis, table_profiles, schema_context
            )
            
            return DictionaryResponse(
                schema_name=f"Database Schema - {domain_analysis.primary_domain.title()} Domain",
                total_tables=len(tables_list),
                total_columns=sum(len(table.get('columns', [])) for table in tables_list),
                total_rows=total_rows,
                domain_analysis=domain_model,
                business_summary=schema_context.business_summary,
                technical_summary=schema_context.technical_summary,
                quality_summary=quality_summary,
                business_workflows=workflows,
                tables=dictionary_tables,
                generated_at=datetime.now().isoformat(),
                ai_engine="SchemaIQ Local AI Agent v1.0",
                confidence_score=overall_confidence
            )
            
        except Exception as e:
            print(f"DEBUG: Exception in generate_intelligent_dictionary: {str(e)}")
            import traceback
            traceback.print_exc()
            return self._build_error_response(str(e))
    
    def _get_table_row_count(self, table_name: str) -> int:
        """
        Get actual row count for a table
        """
        if not self._current_engine:
            return 0
            
        try:
            with self._current_engine.connect() as connection:
                # Use proper SQL escaping for table name
                from sqlalchemy import text
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                return row_count if row_count is not None else 0
        except Exception as e:
            print(f"Warning: Could not get row count for table '{table_name}': {e}")
            return 0
    
    def _extract_sample_data_from_profiles(self, table_profiles: Dict) -> Dict[str, Dict]:
        """
        Extract sample data from profiling results
        """
        sample_data = {}
        
        for table_name, profile in table_profiles.items():
            table_samples = {}
            for column_profile in profile.columns:
                table_samples[column_profile.column_name] = column_profile.sample_values
            sample_data[table_name] = table_samples
        
        return sample_data
    
    def _build_dictionary_column(self, column: Dict, column_context: Any, 
                               column_profile: Any) -> DictionaryColumnModel:
        """
        Build dictionary column model with AI context
        """
        # Basic column info
        column_name = column.get('name', '')
        column_type = column.get('type', 'unknown')
        
        # AI context
        description = "Auto-extracted column"
        business_context = "Pending AI context"
        inferred_content_type = "General data"
        confidence = 0.0
        domain_category = "general"
        
        if column_context:
            description = column_context.description
            business_context = column_context.business_meaning
            confidence = column_context.confidence
            domain_category = column_context.data_category
        
        # Profiling data
        sample_values = []
        null_percentage = 0.0
        unique_count = 0
        unique_percentage = 0.0
        min_length = None
        max_length = None
        avg_length = None
        data_patterns = []
        quality_score = 0.0
        
        if column_profile:
            sample_values = column_profile.sample_values
            null_percentage = column_profile.null_percentage
            unique_count = column_profile.unique_count
            unique_percentage = column_profile.unique_percentage
            min_length = column_profile.min_length
            max_length = column_profile.max_length
            avg_length = column_profile.avg_length
            data_patterns = column_profile.data_patterns
            inferred_content_type = column_profile.inferred_content_type
            quality_score = column_profile.quality_score
        
        # Determine key flags
        primary_key = column_name in column.get('primary_key', [])
        foreign_key = None
        
        # Check for foreign key references
        for fk in column.get('foreign_keys', []):
            if column_name in fk.get('constrained_columns', []):
                ref_table = fk.get('referred_table', '')
                ref_columns = fk.get('referred_columns', [])
                if ref_columns:
                    foreign_key = f"{ref_table}.{ref_columns[0]}"
                break
        
        return DictionaryColumnModel(
            name=column_name,
            type=column_type,
            nullable=not primary_key,  # Assume PKs are not nullable
            primary_key=primary_key,
            foreign_key=foreign_key,
            sample_values=sample_values[:5],  # Limit to 5 samples
            null_percentage=round(null_percentage, 2),
            unique_count=unique_count,
            unique_percentage=round(unique_percentage, 2),
            min_length=min_length,
            max_length=max_length,
            avg_length=round(avg_length, 2) if avg_length else None,
            description=description,
            business_context=business_context,
            inferred_content_type=inferred_content_type,
            data_patterns=data_patterns,
            quality_score=round(quality_score, 3),
            domain_category=domain_category,
            confidence=round(confidence, 3)
        )
    
    def _build_dictionary_table(self, table: Dict, table_context: Any, 
                              table_profile: Any, columns: List[DictionaryColumnModel],
                              relationships: List[Dict]) -> DictionaryTableModel:
        """
        Build dictionary table model with AI context
        """
        table_name = table.get('name', '')
        
        # Basic info - get actual row count from database
        row_count = self._get_table_row_count(table_name)
        column_count = len(columns)
        
        # AI context
        description = "Database table"
        business_purpose = "Data storage and management"
        domain_role = "Supporting entity"
        summary = "Dynamically loaded from database schema"
        relationships_summary = "No relationships defined"
        data_category = "operational_data"
        domain = "general"
        
        if table_context:
            description = table_context.description
            business_purpose = table_context.business_purpose
            domain_role = table_context.domain_role
            relationships_summary = table_context.relationships_summary
            data_category = table_context.data_category
        
        # Profiling data
        data_density = 0.0
        completeness_score = 0.0
        consistency_score = 0.0
        
        if table_profile:
            row_count = table_profile.row_count
            data_density = table_profile.data_density
            completeness_score = table_profile.completeness_score
            consistency_score = table_profile.consistency_score
        
        # Extract keys
        primary_keys = table.get('primary_key', [])
        foreign_keys = table.get('foreign_keys', [])
        
        return DictionaryTableModel(
            name=table_name,
            row_count=row_count,
            column_count=column_count,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            description=description,
            business_purpose=business_purpose,
            domain_role=domain_role,
            summary=summary,
            data_density=round(data_density, 3),
            completeness_score=round(completeness_score, 3),
            consistency_score=round(consistency_score, 3),
            relationships_summary=relationships_summary,
            data_category=data_category,
            domain=domain,
            columns=columns
        )
    
    def _build_domain_analysis_model(self, domain_analysis: Any) -> DomainAnalysisModel:
        """
        Build domain analysis model
        """
        domain_context = self.domain_classifier.get_domain_context(domain_analysis.primary_domain)
        
        secondary_domains = [
            {domain: confidence}
            for domain, confidence in domain_analysis.secondary_domains
        ]
        
        return DomainAnalysisModel(
            primary_domain=domain_analysis.primary_domain,
            confidence=round(domain_analysis.confidence, 3),
            secondary_domains=secondary_domains,
            business_type=domain_analysis.business_type,
            industry_vertical=domain_analysis.industry_vertical,
            complexity_score=domain_analysis.complexity_score,
            description=domain_context.get('description', ''),
            common_entities=domain_context.get('common_entities', [])
        )
    
    def _build_quality_summary(self, profiling_summary: Dict, 
                             table_profiles: Dict) -> DataQualitySummaryModel:
        """
        Build data quality summary
        """
        if not profiling_summary:
            return DataQualitySummaryModel(
                overall_quality=0.0,
                completeness=0.0,
                consistency=0.0,
                data_density=0.0
            )
        
        quality_metrics = profiling_summary.get('quality_metrics', {})
        quality_dist = profiling_summary.get('quality_distribution', {})
        
        # Generate quality issues and recommendations
        issues = []
        recommendations = []
        
        for table_name, profile in table_profiles.items():
            if profile.completeness_score < 0.7:
                issues.append(f"Table '{table_name}' has low data completeness ({profile.completeness_score:.1%})")
                recommendations.append(f"Review data entry processes for table '{table_name}'")
            
            if profile.consistency_score < 0.6:
                issues.append(f"Table '{table_name}' shows data consistency issues")
                recommendations.append(f"Implement data validation rules for table '{table_name}'")
        
        return DataQualitySummaryModel(
            overall_quality=quality_metrics.get('overall_quality', 0.0),
            completeness=quality_metrics.get('avg_completeness', 0.0),
            consistency=quality_metrics.get('avg_consistency', 0.0),
            data_density=quality_metrics.get('avg_data_density', 0.0),
            high_quality_tables=quality_dist.get('high_quality_tables', 0),
            medium_quality_tables=quality_dist.get('medium_quality_tables', 0),
            low_quality_tables=quality_dist.get('low_quality_tables', 0),
            quality_issues=issues[:5],  # Limit to top 5 issues
            recommendations=recommendations[:5]  # Limit to top 5 recommendations
        )
    
    def _build_business_workflows(self, domain_analysis: Any, tables: List[Dict], 
                                relationships: List[Dict]) -> List[BusinessWorkflowModel]:
        """
        Build business workflow models
        """
        workflows = []
        domain = domain_analysis.primary_domain
        
        # Get domain-specific workflows
        domain_workflows = self.context_generator.business_workflows.get(domain, {})
        
        for workflow_name, workflow_desc in domain_workflows.items():
            # Identify involved tables based on workflow keywords
            involved_tables = []
            workflow_keywords = workflow_name.lower().split('_')
            
            for table in tables:
                table_name = table.get('name', '').lower()
                if any(keyword in table_name for keyword in workflow_keywords):
                    involved_tables.append(table.get('name', ''))
            
            workflows.append(BusinessWorkflowModel(
                workflow_name=workflow_name.replace('_', ' ').title(),
                description=f"Business process: {workflow_desc}",
                involved_tables=involved_tables,
                process_flow=workflow_desc
            ))
        
        return workflows
    
    def _calculate_overall_confidence(self, domain_analysis: Any, 
                                    table_profiles: Dict, schema_context: Any) -> float:
        """
        Calculate overall AI confidence score
        """
        confidence_factors = []
        
        # Domain classification confidence
        confidence_factors.append(domain_analysis.confidence)
        
        # Data profiling confidence (based on successful profiling)
        if table_profiles:
            profiling_confidence = len(table_profiles) / len(schema_context.tables)
            confidence_factors.append(profiling_confidence)
        
        # Pattern matching confidence (average from columns)
        pattern_confidences = []
        for table_contexts in schema_context.columns.values():
            for column_context in table_contexts:
                pattern_confidences.append(column_context.confidence)
        
        if pattern_confidences:
            avg_pattern_confidence = sum(pattern_confidences) / len(pattern_confidences)
            confidence_factors.append(avg_pattern_confidence)
        
        # Calculate weighted average
        if confidence_factors:
            overall_confidence = sum(confidence_factors) / len(confidence_factors)
            return round(overall_confidence, 3)
        
        return 0.5  # Default moderate confidence
    
    def _build_error_response(self, error_message: str) -> DictionaryResponse:
        """
        Build minimal error response
        """
        return DictionaryResponse(
            schema_name="Error - Unable to analyze schema",
            total_tables=0,
            total_columns=0,
            total_rows=0,
            domain_analysis=DomainAnalysisModel(
                primary_domain="unknown",
                confidence=0.0,
                business_type="Unknown",
                industry_vertical="Unknown",
                complexity_score=0.0
            ),
            business_summary=f"Error occurred during analysis: {error_message}",
            technical_summary="Unable to generate technical summary due to error",
            quality_summary=DataQualitySummaryModel(
                overall_quality=0.0,
                completeness=0.0,
                consistency=0.0,
                data_density=0.0
            ),
            tables=[],
            generated_at=datetime.now().isoformat(),
            confidence_score=0.0
        )
    
    def enhance_existing_dictionary(self, existing_dict: Dict, 
                                  additional_context: Dict = None) -> DictionaryResponse:
        """
        Enhance existing dictionary with additional AI context
        """
        # This method can be used to enhance dictionaries from external sources
        # with local AI intelligence
        pass
    
    def search_dictionary(self, dictionary: DictionaryResponse, 
                         query: str, search_fields: List[str] = None) -> List[Dict]:
        """
        Search through dictionary content
        """
        # Implement intelligent search functionality
        pass
    
    def export_dictionary(self, dictionary: DictionaryResponse, 
                         format: str = "markdown") -> str:
        """
        Export dictionary in various formats
        """
        # Implement export functionality
        pass
