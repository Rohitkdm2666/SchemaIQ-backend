"""
Advanced Data Profiling Engine
Intelligent data analysis for enhanced context generation
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import re
from sqlalchemy import Engine, text
import statistics

@dataclass
class ColumnProfile:
    column_name: str
    data_type: str
    sample_values: List[str]
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    min_length: Optional[int]
    max_length: Optional[int]
    avg_length: Optional[float]
    data_patterns: List[str]
    inferred_content_type: str
    quality_score: float

@dataclass
class TableProfile:
    table_name: str
    row_count: int
    column_count: int
    columns: List[ColumnProfile]
    data_density: float
    completeness_score: float
    consistency_score: float

class IntelligentDataProfiler:
    """
    Advanced data profiling for context-aware schema analysis
    """
    
    def __init__(self):
        self.content_patterns = self._build_content_patterns()
        self.quality_thresholds = self._build_quality_thresholds()
        
    def _build_content_patterns(self) -> Dict[str, Dict]:
        """
        Patterns for inferring content types from sample data
        """
        return {
            'email': {
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'confidence_threshold': 0.8,
                'description': 'Email addresses for communication'
            },
            'phone': {
                'pattern': r'^[\+]?[1-9]?[\d\s\-\(\)]{7,15}$',
                'confidence_threshold': 0.7,
                'description': 'Phone numbers for contact'
            },
            'url': {
                'pattern': r'^https?://[^\s/$.?#].[^\s]*$|^www\.[^\s/$.?#].[^\s]*$',
                'confidence_threshold': 0.9,
                'description': 'Web URLs and links'
            },
            'ip_address': {
                'pattern': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
                'confidence_threshold': 0.95,
                'description': 'IP addresses for network identification'
            },
            'uuid': {
                'pattern': r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
                'confidence_threshold': 0.95,
                'description': 'Universally unique identifiers'
            },
            'credit_card': {
                'pattern': r'^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3[0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})$',
                'confidence_threshold': 0.9,
                'description': 'Credit card numbers (masked for security)'
            },
            'ssn': {
                'pattern': r'^\d{3}-?\d{2}-?\d{4}$',
                'confidence_threshold': 0.9,
                'description': 'Social security numbers (sensitive data)'
            },
            'postal_code': {
                'pattern': r'^[0-9]{5}(-[0-9]{4})?$|^[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$',
                'confidence_threshold': 0.8,
                'description': 'Postal/ZIP codes for geographic location'
            },
            'currency': {
                'pattern': r'^\$?[0-9]{1,3}(,[0-9]{3})*(\.[0-9]{2})?$',
                'confidence_threshold': 0.7,
                'description': 'Currency amounts and financial values'
            },
            'percentage': {
                'pattern': r'^[0-9]{1,3}(\.[0-9]+)?%?$',
                'confidence_threshold': 0.8,
                'description': 'Percentage values and ratios'
            },
            'date_iso': {
                'pattern': r'^\d{4}-\d{2}-\d{2}$',
                'confidence_threshold': 0.9,
                'description': 'ISO format dates (YYYY-MM-DD)'
            },
            'time_24h': {
                'pattern': r'^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$',
                'confidence_threshold': 0.9,
                'description': '24-hour time format'
            },
            'json': {
                'pattern': r'^[\{\[].*[\}\]]$',
                'confidence_threshold': 0.8,
                'description': 'JSON structured data'
            },
            'xml': {
                'pattern': r'^<[^>]+>.*</[^>]+>$',
                'confidence_threshold': 0.9,
                'description': 'XML structured data'
            },
            'base64': {
                'pattern': r'^[A-Za-z0-9+/]*={0,2}$',
                'confidence_threshold': 0.7,
                'description': 'Base64 encoded data'
            },
            'hash_md5': {
                'pattern': r'^[a-f0-9]{32}$',
                'confidence_threshold': 0.95,
                'description': 'MD5 hash values'
            },
            'hash_sha1': {
                'pattern': r'^[a-f0-9]{40}$',
                'confidence_threshold': 0.95,
                'description': 'SHA1 hash values'
            },
            'hash_sha256': {
                'pattern': r'^[a-f0-9]{64}$',
                'confidence_threshold': 0.95,
                'description': 'SHA256 hash values'
            },
            'coordinates': {
                'pattern': r'^-?[0-9]{1,3}\.[0-9]+,-?[0-9]{1,3}\.[0-9]+$',
                'confidence_threshold': 0.9,
                'description': 'Geographic coordinates (lat,lng)'
            },
            'color_hex': {
                'pattern': r'^#[0-9A-Fa-f]{6}$',
                'confidence_threshold': 0.95,
                'description': 'Hexadecimal color codes'
            },
            'version': {
                'pattern': r'^[0-9]+\.[0-9]+(\.[0-9]+)?(-[a-zA-Z0-9]+)?$',
                'confidence_threshold': 0.8,
                'description': 'Software version numbers'
            },
            'filename': {
                'pattern': r'^.+\.[a-zA-Z0-9]{2,4}$',
                'confidence_threshold': 0.7,
                'description': 'File names with extensions'
            },
            'alphanumeric_code': {
                'pattern': r'^[A-Z0-9]{3,20}$',
                'confidence_threshold': 0.6,
                'description': 'Alphanumeric codes and identifiers'
            },
            'numeric_id': {
                'pattern': r'^[0-9]{3,20}$',
                'confidence_threshold': 0.5,
                'description': 'Numeric identifiers and codes'
            }
        }
    
    def _build_quality_thresholds(self) -> Dict[str, float]:
        """
        Quality assessment thresholds
        """
        return {
            'completeness_excellent': 0.95,
            'completeness_good': 0.85,
            'completeness_fair': 0.70,
            'uniqueness_high': 0.90,
            'uniqueness_medium': 0.50,
            'uniqueness_low': 0.10,
            'consistency_high': 0.90,
            'consistency_medium': 0.70,
            'consistency_low': 0.50
        }
    
    def profile_table(self, engine: Engine, table_name: str, 
                     sample_size: int = 1000) -> TableProfile:
        """
        Comprehensive table profiling with intelligent analysis
        """
        try:
            # Get basic table info
            row_count = self._get_row_count(engine, table_name)
            columns_info = self._get_columns_info(engine, table_name)
            
            # Profile each column
            column_profiles = []
            for column_info in columns_info:
                column_profile = self._profile_column(
                    engine, table_name, column_info, row_count, sample_size
                )
                column_profiles.append(column_profile)
            
            # Calculate table-level metrics
            data_density = self._calculate_data_density(column_profiles, row_count)
            completeness_score = self._calculate_completeness_score(column_profiles)
            consistency_score = self._calculate_consistency_score(column_profiles)
            
            return TableProfile(
                table_name=table_name,
                row_count=row_count,
                column_count=len(column_profiles),
                columns=column_profiles,
                data_density=data_density,
                completeness_score=completeness_score,
                consistency_score=consistency_score
            )
            
        except Exception as e:
            # Return minimal profile on error
            return TableProfile(
                table_name=table_name,
                row_count=0,
                column_count=0,
                columns=[],
                data_density=0.0,
                completeness_score=0.0,
                consistency_score=0.0
            )
    
    def _get_row_count(self, engine: Engine, table_name: str) -> int:
        """
        Get total row count for table
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar() or 0
        except:
            return 0
    
    def _get_columns_info(self, engine: Engine, table_name: str) -> List[Dict]:
        """
        Get column information from database metadata
        """
        try:
            with engine.connect() as conn:
                # Try SQLite-specific query first
                try:
                    result = conn.execute(text(f"PRAGMA table_info({table_name})"))
                    columns = []
                    for row in result:
                        columns.append({
                            'name': row[1],  # column name
                            'type': row[2],  # data type
                            'nullable': not bool(row[3])  # not null flag
                        })
                    return columns
                except:
                    # Fallback to generic approach
                    from sqlalchemy import inspect
                    inspector = inspect(engine)
                    columns_meta = inspector.get_columns(table_name)
                    return [
                        {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col.get('nullable', True)
                        }
                        for col in columns_meta
                    ]
        except:
            return []
    
    def _profile_column(self, engine: Engine, table_name: str, column_info: Dict,
                       total_rows: int, sample_size: int) -> ColumnProfile:
        """
        Comprehensive column profiling
        """
        column_name = column_info['name']
        data_type = column_info['type']
        
        try:
            with engine.connect() as conn:
                # Get sample data and statistics
                sample_query = f"""
                SELECT {column_name}, 
                       COUNT(*) as count,
                       COUNT({column_name}) as non_null_count
                FROM {table_name} 
                WHERE {column_name} IS NOT NULL 
                GROUP BY {column_name}
                ORDER BY count DESC 
                LIMIT {sample_size}
                """
                
                result = conn.execute(text(sample_query))
                rows = result.fetchall()
                
                # Extract sample values
                sample_values = [str(row[0]) for row in rows if row[0] is not None][:10]
                
                # Get null count
                null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
                null_count = conn.execute(text(null_query)).scalar() or 0
                
                # Get unique count
                unique_query = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}"
                unique_count = conn.execute(text(unique_query)).scalar() or 0
                
        except Exception as e:
            # Fallback values on error
            sample_values = []
            null_count = 0
            unique_count = 0
        
        # Calculate metrics
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        unique_percentage = (unique_count / total_rows * 100) if total_rows > 0 else 0
        
        # Analyze text lengths (for string columns)
        lengths = []
        if sample_values:
            lengths = [len(str(val)) for val in sample_values]
        
        min_length = min(lengths) if lengths else None
        max_length = max(lengths) if lengths else None
        avg_length = statistics.mean(lengths) if lengths else None
        
        # Detect data patterns
        data_patterns = self._detect_data_patterns(sample_values)
        
        # Infer content type
        inferred_content_type = self._infer_content_type(sample_values, column_name, data_type)
        
        # Calculate quality score
        quality_score = self._calculate_column_quality_score(
            null_percentage, unique_percentage, data_patterns, sample_values
        )
        
        return ColumnProfile(
            column_name=column_name,
            data_type=data_type,
            sample_values=sample_values,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage,
            min_length=min_length,
            max_length=max_length,
            avg_length=avg_length,
            data_patterns=data_patterns,
            inferred_content_type=inferred_content_type,
            quality_score=quality_score
        )
    
    def _detect_data_patterns(self, sample_values: List[str]) -> List[str]:
        """
        Detect data patterns in sample values
        """
        if not sample_values:
            return []
        
        detected_patterns = []
        
        for pattern_name, pattern_info in self.content_patterns.items():
            pattern = pattern_info['pattern']
            threshold = pattern_info['confidence_threshold']
            
            matches = 0
            for value in sample_values:
                if value and re.match(pattern, str(value)):
                    matches += 1
            
            confidence = matches / len(sample_values) if sample_values else 0
            
            if confidence >= threshold:
                detected_patterns.append(f"{pattern_name} ({confidence:.0%})")
        
        return detected_patterns
    
    def _infer_content_type(self, sample_values: List[str], column_name: str, 
                           data_type: str) -> str:
        """
        Infer semantic content type from patterns and context
        """
        if not sample_values:
            return "No data available"
        
        # Check for high-confidence patterns first
        for pattern_name, pattern_info in self.content_patterns.items():
            pattern = pattern_info['pattern']
            threshold = pattern_info['confidence_threshold']
            
            matches = sum(1 for val in sample_values if val and re.match(pattern, str(val)))
            confidence = matches / len(sample_values) if sample_values else 0
            
            if confidence >= threshold:
                return pattern_info['description']
        
        # Fallback to column name analysis
        name_lower = column_name.lower()
        
        if any(word in name_lower for word in ['email', 'mail']):
            return "Email addresses"
        elif any(word in name_lower for word in ['phone', 'tel', 'mobile']):
            return "Phone numbers"
        elif any(word in name_lower for word in ['url', 'link', 'website']):
            return "Web URLs"
        elif any(word in name_lower for word in ['address', 'street', 'location']):
            return "Physical addresses"
        elif any(word in name_lower for word in ['name', 'title', 'label']):
            return "Names and titles"
        elif any(word in name_lower for word in ['desc', 'comment', 'note']):
            return "Descriptive text"
        elif any(word in name_lower for word in ['code', 'id', 'key']):
            return "Identifier codes"
        elif any(word in name_lower for word in ['date', 'time', 'timestamp']):
            return "Date/time information"
        elif any(word in name_lower for word in ['amount', 'price', 'cost', 'value']):
            return "Monetary values"
        elif any(word in name_lower for word in ['count', 'number', 'quantity']):
            return "Numeric quantities"
        elif any(word in name_lower for word in ['status', 'state', 'flag']):
            return "Status indicators"
        else:
            # Analyze data type
            type_lower = data_type.lower()
            if any(t in type_lower for t in ['varchar', 'text', 'char']):
                return "Text data"
            elif any(t in type_lower for t in ['int', 'number', 'numeric']):
                return "Numeric data"
            elif any(t in type_lower for t in ['date', 'time']):
                return "Temporal data"
            elif any(t in type_lower for t in ['bool', 'bit']):
                return "Boolean flags"
            else:
                return "General data"
    
    def _calculate_column_quality_score(self, null_percentage: float, unique_percentage: float,
                                      data_patterns: List[str], sample_values: List[str]) -> float:
        """
        Calculate overall quality score for column
        """
        score = 1.0
        
        # Completeness factor (penalize high null rates)
        if null_percentage > 50:
            score -= 0.4
        elif null_percentage > 20:
            score -= 0.2
        elif null_percentage > 5:
            score -= 0.1
        
        # Consistency factor (reward pattern detection)
        if data_patterns:
            score += 0.1 * len(data_patterns)
        
        # Data richness factor (reward variety in reasonable range)
        if 10 <= unique_percentage <= 90:
            score += 0.1
        elif unique_percentage < 5 or unique_percentage > 95:
            score -= 0.1
        
        # Sample quality factor
        if sample_values:
            # Penalize very short or very long average lengths for text
            avg_len = sum(len(str(val)) for val in sample_values) / len(sample_values)
            if 5 <= avg_len <= 100:
                score += 0.05
        
        return max(0.0, min(1.0, score))
    
    def _calculate_data_density(self, column_profiles: List[ColumnProfile], 
                               total_rows: int) -> float:
        """
        Calculate overall data density (non-null percentage)
        """
        if not column_profiles or total_rows == 0:
            return 0.0
        
        total_cells = len(column_profiles) * total_rows
        null_cells = sum(profile.null_count for profile in column_profiles)
        
        return ((total_cells - null_cells) / total_cells) if total_cells > 0 else 0.0
    
    def _calculate_completeness_score(self, column_profiles: List[ColumnProfile]) -> float:
        """
        Calculate overall completeness score
        """
        if not column_profiles:
            return 0.0
        
        completeness_scores = []
        for profile in column_profiles:
            completeness = (100 - profile.null_percentage) / 100
            completeness_scores.append(completeness)
        
        return statistics.mean(completeness_scores)
    
    def _calculate_consistency_score(self, column_profiles: List[ColumnProfile]) -> float:
        """
        Calculate overall consistency score based on pattern detection
        """
        if not column_profiles:
            return 0.0
        
        consistency_scores = []
        for profile in column_profiles:
            # Base consistency on pattern detection and quality score
            if profile.data_patterns:
                consistency = profile.quality_score
            else:
                # Lower consistency if no patterns detected
                consistency = profile.quality_score * 0.7
            
            consistency_scores.append(consistency)
        
        return statistics.mean(consistency_scores)
    
    def get_profiling_summary(self, table_profiles: List[TableProfile]) -> Dict[str, Any]:
        """
        Generate comprehensive profiling summary
        """
        if not table_profiles:
            return {}
        
        total_tables = len(table_profiles)
        total_columns = sum(profile.column_count for profile in table_profiles)
        total_rows = sum(profile.row_count for profile in table_profiles)
        
        # Average scores
        avg_completeness = statistics.mean([p.completeness_score for p in table_profiles])
        avg_consistency = statistics.mean([p.consistency_score for p in table_profiles])
        avg_density = statistics.mean([p.data_density for p in table_profiles])
        
        # Quality distribution
        high_quality_tables = sum(1 for p in table_profiles if p.completeness_score > 0.9)
        medium_quality_tables = sum(1 for p in table_profiles if 0.7 <= p.completeness_score <= 0.9)
        low_quality_tables = sum(1 for p in table_profiles if p.completeness_score < 0.7)
        
        # Content type analysis
        content_types = {}
        for profile in table_profiles:
            for col_profile in profile.columns:
                content_type = col_profile.inferred_content_type
                content_types[content_type] = content_types.get(content_type, 0) + 1
        
        return {
            'overview': {
                'total_tables': total_tables,
                'total_columns': total_columns,
                'total_rows': total_rows,
                'avg_columns_per_table': total_columns / total_tables if total_tables > 0 else 0
            },
            'quality_metrics': {
                'avg_completeness': round(avg_completeness, 3),
                'avg_consistency': round(avg_consistency, 3),
                'avg_data_density': round(avg_density, 3),
                'overall_quality': round((avg_completeness + avg_consistency + avg_density) / 3, 3)
            },
            'quality_distribution': {
                'high_quality_tables': high_quality_tables,
                'medium_quality_tables': medium_quality_tables,
                'low_quality_tables': low_quality_tables
            },
            'content_analysis': {
                'detected_content_types': len(content_types),
                'top_content_types': sorted(content_types.items(), key=lambda x: x[1], reverse=True)[:5]
            }
        }
