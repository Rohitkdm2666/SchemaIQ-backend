"""
Universal Database Intelligence Agent - Pattern Recognition Engine
Comprehensive pattern matching for ANY database schema in the world
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class ColumnContext:
    description: str
    business_meaning: str
    domain: str
    confidence: float
    data_category: str

class UniversalPatternEngine:
    """
    Universal pattern recognition engine that can understand ANY database column
    across ALL business domains and use cases worldwide
    """
    
    def __init__(self):
        self.patterns = self._build_comprehensive_patterns()
        self.domain_keywords = self._build_domain_keywords()
        self.data_type_contexts = self._build_data_type_contexts()
        
    def _build_comprehensive_patterns(self) -> Dict[str, Dict]:
        """
        Comprehensive pattern database covering ALL possible column naming conventions
        """
        return {
            # ═══════════════════════════════════════════════════════════════
            # IDENTITY & KEYS
            # ═══════════════════════════════════════════════════════════════
            r'^id$|^ID$': {
                'desc': 'Primary key identifier for this record',
                'business': 'Unique system-generated identifier used for database relationships and record tracking',
                'category': 'identity'
            },
            r'.*_id$|.*_ID$|.*Id$': {
                'desc': 'Foreign key reference to {referenced_table} table',
                'business': 'Links this record to related data in the {referenced_table} system',
                'category': 'identity'
            },
            r'^uuid$|^guid$|^GUID$|^UUID$': {
                'desc': 'Globally unique identifier (UUID/GUID)',
                'business': 'Universally unique identifier ensuring no conflicts across distributed systems',
                'category': 'identity'
            },
            r'.*_key$|.*_code$|.*_ref$|.*_reference$': {
                'desc': 'Reference key or code for external system integration',
                'business': 'External reference identifier for system integration and data synchronization',
                'category': 'identity'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # TEMPORAL DATA
            # ═══════════════════════════════════════════════════════════════
            r'.*_at$|.*_time$|.*_timestamp$': {
                'desc': 'Timestamp indicating when {action} occurred',
                'business': 'Critical timing information for audit trails, workflows, and business process tracking',
                'category': 'temporal'
            },
            r'.*_date$|.*_dt$|.*Date$': {
                'desc': 'Date when {action} was recorded or occurred',
                'business': 'Important date information for business reporting, compliance, and historical analysis',
                'category': 'temporal'
            },
            r'^created.*|^insert.*|^add.*': {
                'desc': 'Record creation timestamp',
                'business': 'Tracks when this record was first created in the system for audit and compliance',
                'category': 'temporal'
            },
            r'^updated.*|^modified.*|^changed.*|^edit.*': {
                'desc': 'Last modification timestamp',
                'business': 'Tracks the most recent update to this record for change management and auditing',
                'category': 'temporal'
            },
            r'^deleted.*|^removed.*|^archived.*': {
                'desc': 'Soft deletion or archival timestamp',
                'business': 'Indicates when record was marked as deleted while preserving historical data',
                'category': 'temporal'
            },
            r'.*_start.*|.*_begin.*|.*_from.*': {
                'desc': 'Start date/time for period or event',
                'business': 'Defines the beginning of a time period, contract, or business process',
                'category': 'temporal'
            },
            r'.*_end.*|.*_finish.*|.*_to.*|.*_until.*|.*_expir.*': {
                'desc': 'End date/time for period or event',
                'business': 'Defines the conclusion of a time period, contract, or business process',
                'category': 'temporal'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # CONTACT & PERSONAL INFORMATION
            # ═══════════════════════════════════════════════════════════════
            r'^email$|.*_email$|.*email.*|.*@.*': {
                'desc': 'Email address for digital communication',
                'business': 'Primary digital contact method for customer communication, notifications, and marketing',
                'category': 'contact'
            },
            r'^phone$|.*_phone$|.*telephone$|.*mobile$|.*cell$': {
                'desc': 'Phone number for voice communication',
                'business': 'Voice contact information for customer support, emergency contact, and verification',
                'category': 'contact'
            },
            r'^name$|.*_name$|^first.*|^last.*|^full.*name.*': {
                'desc': 'Personal or entity name information',
                'business': 'Human-readable identifier for personalization, communication, and record identification',
                'category': 'personal'
            },
            r'^address$|.*_address$|.*street.*|.*addr.*': {
                'desc': 'Physical mailing or street address',
                'business': 'Location information for shipping, billing, service delivery, and legal compliance',
                'category': 'contact'
            },
            r'^city$|.*_city$|^town$|.*_town$': {
                'desc': 'City or town location',
                'business': 'Geographic location for regional analysis, shipping zones, and local compliance',
                'category': 'geographic'
            },
            r'^state$|.*_state$|^province$|.*_province$|^region$': {
                'desc': 'State, province, or regional location',
                'business': 'Administrative region for tax calculation, shipping rules, and regulatory compliance',
                'category': 'geographic'
            },
            r'^zip$|.*_zip$|^postal.*|.*postal.*|^pincode$': {
                'desc': 'Postal or ZIP code',
                'business': 'Postal code for accurate delivery, geographic analysis, and location-based services',
                'category': 'geographic'
            },
            r'^country$|.*_country$|^nation$': {
                'desc': 'Country or nation identifier',
                'business': 'National location for international operations, currency, and regulatory compliance',
                'category': 'geographic'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # FINANCIAL DATA
            # ═══════════════════════════════════════════════════════════════
            r'.*amount$|.*_amt$|^amount$': {
                'desc': 'Monetary amount or financial value',
                'business': 'Financial value critical for accounting, revenue tracking, and financial reporting',
                'category': 'financial'
            },
            r'.*price$|.*_price$|^price$|.*cost$|.*_cost$': {
                'desc': 'Price or cost information',
                'business': 'Pricing data essential for revenue calculation, profitability analysis, and customer billing',
                'category': 'financial'
            },
            r'.*salary$|.*wage$|.*pay$|.*compensation$': {
                'desc': 'Salary, wage, or compensation amount',
                'business': 'Employee compensation data for payroll processing and HR management',
                'category': 'financial'
            },
            r'.*tax$|.*_tax$|.*vat$|.*gst$': {
                'desc': 'Tax amount or tax-related information',
                'business': 'Tax calculation data for compliance, accounting, and financial reporting',
                'category': 'financial'
            },
            r'.*discount$|.*_discount$|.*rebate$': {
                'desc': 'Discount or rebate amount',
                'business': 'Promotional pricing adjustments affecting revenue and customer incentives',
                'category': 'financial'
            },
            r'.*balance$|.*_balance$|^balance$': {
                'desc': 'Account balance or remaining amount',
                'business': 'Current financial position critical for account management and credit decisions',
                'category': 'financial'
            },
            r'.*currency$|.*_currency$|^currency$': {
                'desc': 'Currency type or currency code',
                'business': 'Currency denomination for international transactions and exchange rate calculations',
                'category': 'financial'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # STATUS & STATE MANAGEMENT
            # ═══════════════════════════════════════════════════════════════
            r'.*status$|.*_status$|^status$': {
                'desc': 'Current status or state of the record',
                'business': 'Workflow state critical for business process management and operational decisions',
                'category': 'status'
            },
            r'.*state$|.*_state$|^state$': {
                'desc': 'Processing state or condition',
                'business': 'Current condition affecting business logic, workflows, and system behavior',
                'category': 'status'
            },
            r'.*active$|.*_active$|^active$|.*enabled$': {
                'desc': 'Active/inactive flag or enabled status',
                'business': 'Activation state controlling system behavior and business rule application',
                'category': 'status'
            },
            r'.*approved$|.*_approved$|.*verified$|.*confirmed$': {
                'desc': 'Approval or verification status',
                'business': 'Authorization state critical for compliance, security, and business process control',
                'category': 'status'
            },
            r'.*priority$|.*_priority$|^priority$|.*urgent$': {
                'desc': 'Priority level or urgency indicator',
                'business': 'Priority classification for resource allocation and workflow management',
                'category': 'status'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # BUSINESS METRICS & ANALYTICS
            # ═══════════════════════════════════════════════════════════════
            r'.*count$|.*_count$|^count$|.*quantity$|.*qty$': {
                'desc': 'Count, quantity, or numerical measure',
                'business': 'Quantitative data essential for inventory management, analytics, and reporting',
                'category': 'metrics'
            },
            r'.*score$|.*_score$|^score$|.*rating$|.*rank$': {
                'desc': 'Score, rating, or ranking value',
                'business': 'Performance metric for evaluation, comparison, and decision-making processes',
                'category': 'metrics'
            },
            r'.*percentage$|.*_pct$|.*percent$|.*rate$': {
                'desc': 'Percentage or rate measurement',
                'business': 'Proportional metric for performance analysis, conversion tracking, and KPI monitoring',
                'category': 'metrics'
            },
            r'.*weight$|.*_weight$|.*size$|.*_size$|.*length$|.*width$|.*height$': {
                'desc': 'Physical measurement or dimension',
                'business': 'Physical specifications for shipping, manufacturing, and product management',
                'category': 'metrics'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # HEALTHCARE & MEDICAL
            # ═══════════════════════════════════════════════════════════════
            r'.*patient.*|.*medical.*|.*diagnosis.*|.*symptom.*': {
                'desc': 'Medical or healthcare-related information',
                'business': 'Critical healthcare data for patient care, medical records, and clinical decisions',
                'category': 'healthcare'
            },
            r'.*blood.*|.*pressure.*|.*heart.*|.*pulse.*': {
                'desc': 'Vital signs or medical measurement',
                'business': 'Clinical data essential for patient monitoring and medical assessment',
                'category': 'healthcare'
            },
            r'.*drug.*|.*medication.*|.*prescription.*|.*dose.*': {
                'desc': 'Pharmaceutical or medication information',
                'business': 'Drug therapy data critical for patient safety and treatment effectiveness',
                'category': 'healthcare'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # EDUCATION & ACADEMIC
            # ═══════════════════════════════════════════════════════════════
            r'.*student.*|.*grade.*|.*course.*|.*class.*|.*subject.*': {
                'desc': 'Educational or academic information',
                'business': 'Academic data for student management, performance tracking, and educational analytics',
                'category': 'education'
            },
            r'.*exam.*|.*test.*|.*quiz.*|.*assignment.*': {
                'desc': 'Assessment or evaluation data',
                'business': 'Academic assessment information for performance evaluation and progress tracking',
                'category': 'education'
            },
            r'.*teacher.*|.*instructor.*|.*professor.*|.*faculty.*': {
                'desc': 'Educational staff information',
                'business': 'Faculty data for academic administration and educational resource management',
                'category': 'education'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # MANUFACTURING & INVENTORY
            # ═══════════════════════════════════════════════════════════════
            r'.*product.*|.*item.*|.*sku.*|.*part.*|.*component.*': {
                'desc': 'Product or inventory item information',
                'business': 'Product data essential for inventory management, sales, and supply chain operations',
                'category': 'inventory'
            },
            r'.*stock.*|.*inventory.*|.*warehouse.*|.*shelf.*': {
                'desc': 'Inventory or stock management data',
                'business': 'Stock information critical for supply chain management and order fulfillment',
                'category': 'inventory'
            },
            r'.*serial.*|.*batch.*|.*lot.*|.*model.*': {
                'desc': 'Product identification or batch information',
                'business': 'Traceability data for quality control, recalls, and manufacturing compliance',
                'category': 'inventory'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # TECHNOLOGY & SYSTEMS
            # ═══════════════════════════════════════════════════════════════
            r'.*url$|.*_url$|^url$|.*link.*|.*href.*': {
                'desc': 'Web URL or hyperlink reference',
                'business': 'Digital resource location for web integration, content management, and user navigation',
                'category': 'technology'
            },
            r'.*ip.*|.*server.*|.*host.*|.*domain.*': {
                'desc': 'Network or server identification',
                'business': 'Technical infrastructure data for system administration and network management',
                'category': 'technology'
            },
            r'.*token$|.*_token$|.*session.*|.*auth.*': {
                'desc': 'Authentication or session token',
                'business': 'Security credential for user authentication and system access control',
                'category': 'technology'
            },
            r'.*version$|.*_version$|.*revision$|.*build$': {
                'desc': 'Version or revision information',
                'business': 'Version control data for software management and change tracking',
                'category': 'technology'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # LEGAL & COMPLIANCE
            # ═══════════════════════════════════════════════════════════════
            r'.*license.*|.*permit.*|.*certificate.*|.*compliance.*': {
                'desc': 'Legal license or compliance information',
                'business': 'Regulatory compliance data essential for legal operations and audit requirements',
                'category': 'legal'
            },
            r'.*contract.*|.*agreement.*|.*terms.*|.*policy.*': {
                'desc': 'Legal contract or policy information',
                'business': 'Legal documentation data for contract management and policy enforcement',
                'category': 'legal'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # SOCIAL & COMMUNICATION
            # ═══════════════════════════════════════════════════════════════
            r'.*message.*|.*comment.*|.*note.*|.*description.*|.*desc.*': {
                'desc': 'Text message, comment, or descriptive information',
                'business': 'Communication data for customer service, feedback analysis, and information sharing',
                'category': 'communication'
            },
            r'.*social.*|.*twitter.*|.*facebook.*|.*linkedin.*|.*instagram.*': {
                'desc': 'Social media or social network information',
                'business': 'Social media data for marketing, customer engagement, and brand management',
                'category': 'social'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # GAMING & ENTERTAINMENT
            # ═══════════════════════════════════════════════════════════════
            r'.*game.*|.*player.*|.*level.*|.*achievement.*|.*score.*': {
                'desc': 'Gaming or entertainment-related data',
                'business': 'Gaming metrics for player engagement, progression tracking, and entertainment analytics',
                'category': 'gaming'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # TRANSPORTATION & LOGISTICS
            # ═══════════════════════════════════════════════════════════════
            r'.*shipping.*|.*delivery.*|.*transport.*|.*vehicle.*|.*route.*': {
                'desc': 'Transportation or logistics information',
                'business': 'Logistics data for shipping management, delivery tracking, and transportation optimization',
                'category': 'logistics'
            },
            r'.*tracking.*|.*carrier.*|.*freight.*|.*cargo.*': {
                'desc': 'Shipment tracking or cargo information',
                'business': 'Shipment data for delivery management and logistics coordination',
                'category': 'logistics'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # REAL ESTATE & PROPERTY
            # ═══════════════════════════════════════════════════════════════
            r'.*property.*|.*building.*|.*apartment.*|.*room.*|.*floor.*': {
                'desc': 'Real estate or property information',
                'business': 'Property data for real estate management, facility operations, and space utilization',
                'category': 'real_estate'
            },
            
            # ═══════════════════════════════════════════════════════════════
            # CATCH-ALL PATTERNS
            # ═══════════════════════════════════════════════════════════════
            r'.*': {
                'desc': 'Data field containing {inferred_content}',
                'business': 'Business data element contributing to operational processes and decision-making',
                'category': 'general'
            }
        }
    
    def _build_domain_keywords(self) -> Dict[str, List[str]]:
        """
        Comprehensive domain classification keywords
        """
        return {
            'ecommerce': [
                'order', 'product', 'customer', 'payment', 'cart', 'checkout', 'shipping',
                'inventory', 'catalog', 'purchase', 'sale', 'discount', 'coupon', 'review'
            ],
            'finance': [
                'account', 'transaction', 'balance', 'payment', 'loan', 'credit', 'debit',
                'investment', 'portfolio', 'bank', 'currency', 'exchange', 'interest'
            ],
            'healthcare': [
                'patient', 'doctor', 'medical', 'diagnosis', 'treatment', 'prescription',
                'hospital', 'clinic', 'appointment', 'insurance', 'vital', 'symptom'
            ],
            'education': [
                'student', 'teacher', 'course', 'class', 'grade', 'exam', 'assignment',
                'school', 'university', 'enrollment', 'curriculum', 'academic'
            ],
            'hr': [
                'employee', 'staff', 'department', 'salary', 'payroll', 'benefit',
                'performance', 'attendance', 'leave', 'recruitment', 'training'
            ],
            'crm': [
                'lead', 'prospect', 'contact', 'opportunity', 'deal', 'pipeline',
                'campaign', 'marketing', 'sales', 'customer', 'relationship'
            ],
            'manufacturing': [
                'production', 'factory', 'machine', 'assembly', 'quality', 'batch',
                'material', 'component', 'supplier', 'warehouse', 'logistics'
            ],
            'real_estate': [
                'property', 'building', 'apartment', 'lease', 'rent', 'tenant',
                'landlord', 'mortgage', 'listing', 'agent', 'commission'
            ],
            'gaming': [
                'player', 'game', 'level', 'achievement', 'score', 'leaderboard',
                'tournament', 'match', 'character', 'item', 'guild'
            ],
            'social_media': [
                'user', 'post', 'comment', 'like', 'share', 'follow', 'friend',
                'message', 'notification', 'feed', 'timeline', 'profile'
            ],
            'logistics': [
                'shipment', 'delivery', 'carrier', 'tracking', 'warehouse', 'route',
                'vehicle', 'driver', 'freight', 'cargo', 'manifest'
            ],
            'legal': [
                'case', 'client', 'lawyer', 'court', 'contract', 'agreement',
                'compliance', 'regulation', 'license', 'permit', 'audit'
            ],
            'hospitality': [
                'guest', 'room', 'reservation', 'booking', 'hotel', 'restaurant',
                'service', 'amenity', 'check_in', 'check_out', 'housekeeping'
            ],
            'agriculture': [
                'farm', 'crop', 'livestock', 'harvest', 'field', 'farmer',
                'irrigation', 'fertilizer', 'pesticide', 'yield', 'season'
            ],
            'energy': [
                'power', 'electricity', 'grid', 'consumption', 'generation', 'meter',
                'utility', 'renewable', 'solar', 'wind', 'nuclear', 'coal'
            ],
            'telecommunications': [
                'call', 'phone', 'network', 'signal', 'tower', 'subscriber',
                'plan', 'usage', 'roaming', 'bandwidth', 'data', 'voice'
            ],
            'insurance': [
                'policy', 'claim', 'premium', 'coverage', 'beneficiary', 'deductible',
                'underwriting', 'risk', 'adjuster', 'settlement', 'liability'
            ],
            'media': [
                'content', 'article', 'video', 'audio', 'publication', 'author',
                'editor', 'subscriber', 'viewer', 'rating', 'channel', 'broadcast'
            ],
            'government': [
                'citizen', 'service', 'department', 'office', 'permit', 'license',
                'tax', 'regulation', 'policy', 'public', 'municipal', 'federal'
            ]
        }
    
    def _build_data_type_contexts(self) -> Dict[str, Dict]:
        """
        Data type-based context inference
        """
        return {
            'varchar': {
                'short': 'Short text field for names, codes, or brief descriptions',
                'medium': 'Text field for descriptions, comments, or medium-length content',
                'long': 'Long text field for detailed descriptions, articles, or extensive content'
            },
            'text': {
                'default': 'Large text field for extensive content, documents, or detailed information'
            },
            'int': {
                'default': 'Numeric integer value for counts, quantities, or whole number measurements'
            },
            'decimal': {
                'default': 'Precise decimal number for financial amounts, measurements, or calculated values'
            },
            'float': {
                'default': 'Floating-point number for scientific calculations, ratios, or approximate values'
            },
            'boolean': {
                'default': 'True/false flag indicating a binary state or condition'
            },
            'datetime': {
                'default': 'Date and time information for temporal tracking and scheduling'
            },
            'date': {
                'default': 'Date information for calendar-based tracking and reporting'
            },
            'time': {
                'default': 'Time information for scheduling and time-based operations'
            },
            'json': {
                'default': 'Structured JSON data for complex, nested information storage'
            },
            'blob': {
                'default': 'Binary data storage for files, images, or encoded content'
            }
        }

    def analyze_column(self, column_name: str, data_type: str, table_name: str = "", 
                      sample_values: List[str] = None, relationships: List[Dict] = None) -> ColumnContext:
        """
        Comprehensive column analysis using universal pattern recognition
        """
        column_name_lower = column_name.lower()
        table_name_lower = table_name.lower()
        
        # Find best matching pattern
        best_match = None
        highest_confidence = 0.0
        
        for pattern, context in self.patterns.items():
            if re.match(pattern, column_name_lower):
                confidence = self._calculate_confidence(pattern, column_name_lower, data_type, sample_values)
                if confidence > highest_confidence:
                    highest_confidence = confidence
                    best_match = context
        
        if not best_match:
            best_match = self.patterns[r'.*']  # Fallback to catch-all
            highest_confidence = 0.3
        
        # Enhance with data type context
        type_context = self._get_type_context(data_type, sample_values)
        
        # Enhance with relationship context
        relationship_context = self._get_relationship_context(column_name, relationships)
        
        # Generate final description and business meaning
        description = self._generate_description(best_match, column_name, table_name, type_context)
        business_meaning = self._generate_business_meaning(best_match, column_name, table_name, relationship_context)
        
        # Determine domain
        domain = self._classify_domain(table_name, column_name, sample_values)
        
        return ColumnContext(
            description=description,
            business_meaning=business_meaning,
            domain=domain,
            confidence=highest_confidence,
            data_category=best_match.get('category', 'general')
        )
    
    def _calculate_confidence(self, pattern: str, column_name: str, data_type: str, 
                            sample_values: List[str] = None) -> float:
        """
        Calculate confidence score for pattern match
        """
        base_confidence = 0.7
        
        # Exact matches get higher confidence
        if pattern == f'^{re.escape(column_name)}$':
            base_confidence = 0.95
        elif pattern.startswith('^') and pattern.endswith('$'):
            base_confidence = 0.9
        elif pattern.startswith('^') or pattern.endswith('$'):
            base_confidence = 0.8
        
        # Data type consistency bonus
        if self._is_data_type_consistent(pattern, data_type):
            base_confidence += 0.1
        
        # Sample value consistency bonus
        if sample_values and self._are_samples_consistent(pattern, sample_values):
            base_confidence += 0.1
        
        return min(base_confidence, 1.0)
    
    def _is_data_type_consistent(self, pattern: str, data_type: str) -> bool:
        """
        Check if data type is consistent with pattern expectations
        """
        type_lower = data_type.lower()
        
        # Financial patterns should be numeric
        if any(financial in pattern for financial in ['amount', 'price', 'cost', 'salary']):
            return any(num_type in type_lower for num_type in ['int', 'decimal', 'float', 'numeric'])
        
        # Date patterns should be temporal
        if any(temporal in pattern for temporal in ['_date', '_at', '_time']):
            return any(time_type in type_lower for time_type in ['date', 'time', 'timestamp'])
        
        # Email patterns should be text
        if 'email' in pattern:
            return any(text_type in type_lower for text_type in ['varchar', 'text', 'string'])
        
        return True  # Default to consistent
    
    def _are_samples_consistent(self, pattern: str, sample_values: List[str]) -> bool:
        """
        Check if sample values are consistent with pattern expectations
        """
        if not sample_values:
            return True
        
        # Email pattern validation
        if 'email' in pattern:
            return any('@' in str(val) for val in sample_values if val)
        
        # URL pattern validation
        if 'url' in pattern:
            return any(str(val).startswith(('http', 'https', 'www')) for val in sample_values if val)
        
        # Phone pattern validation
        if 'phone' in pattern:
            return any(re.search(r'[\d\-\+\(\)\s]{7,}', str(val)) for val in sample_values if val)
        
        return True  # Default to consistent
    
    def _get_type_context(self, data_type: str, sample_values: List[str] = None) -> str:
        """
        Get context based on data type
        """
        type_lower = data_type.lower()
        
        for type_key, contexts in self.data_type_contexts.items():
            if type_key in type_lower:
                if type_key == 'varchar' and sample_values:
                    avg_length = sum(len(str(val)) for val in sample_values if val) / len([v for v in sample_values if v])
                    if avg_length < 50:
                        return contexts.get('short', contexts.get('default', ''))
                    elif avg_length < 200:
                        return contexts.get('medium', contexts.get('default', ''))
                    else:
                        return contexts.get('long', contexts.get('default', ''))
                return contexts.get('default', '')
        
        return 'Data storage field'
    
    def _get_relationship_context(self, column_name: str, relationships: List[Dict] = None) -> str:
        """
        Get context based on relationships
        """
        if not relationships:
            return ""
        
        # Check if this column is involved in relationships
        for rel in relationships:
            if rel.get('from_column') == column_name:
                return f"References {rel.get('to_table', 'related table')}"
            elif rel.get('to_column') == column_name:
                return f"Referenced by {rel.get('from_table', 'related table')}"
        
        return ""
    
    def _generate_description(self, match_context: Dict, column_name: str, 
                            table_name: str, type_context: str) -> str:
        """
        Generate intelligent column description
        """
        base_desc = match_context.get('desc', 'Data field')
        
        # Replace placeholders
        desc = base_desc.replace('{action}', self._infer_action(column_name))
        desc = desc.replace('{referenced_table}', self._infer_referenced_table(column_name))
        desc = desc.replace('{table}', table_name or 'record')
        desc = desc.replace('{inferred_content}', self._infer_content_type(column_name, type_context))
        
        return desc
    
    def _generate_business_meaning(self, match_context: Dict, column_name: str, 
                                 table_name: str, relationship_context: str) -> str:
        """
        Generate business context and meaning
        """
        base_meaning = match_context.get('business', 'Business data element')
        
        # Replace placeholders
        meaning = base_meaning.replace('{referenced_table}', self._infer_referenced_table(column_name))
        meaning = meaning.replace('{table}', table_name or 'system')
        
        # Add relationship context if available
        if relationship_context:
            meaning += f". {relationship_context}."
        
        return meaning
    
    def _infer_action(self, column_name: str) -> str:
        """
        Infer action from column name
        """
        name_lower = column_name.lower()
        
        if 'created' in name_lower or 'insert' in name_lower:
            return 'record creation'
        elif 'updated' in name_lower or 'modified' in name_lower:
            return 'record modification'
        elif 'deleted' in name_lower or 'removed' in name_lower:
            return 'record deletion'
        elif 'login' in name_lower or 'signin' in name_lower:
            return 'user login'
        elif 'logout' in name_lower or 'signout' in name_lower:
            return 'user logout'
        elif 'purchase' in name_lower or 'order' in name_lower:
            return 'purchase transaction'
        elif 'payment' in name_lower:
            return 'payment processing'
        elif 'ship' in name_lower:
            return 'shipment processing'
        else:
            return 'the related action'
    
    def _infer_referenced_table(self, column_name: str) -> str:
        """
        Infer referenced table from foreign key column name
        """
        if column_name.endswith('_id'):
            table_name = column_name[:-3]
            # Handle pluralization
            if table_name.endswith('s'):
                return table_name
            else:
                return f"{table_name}s"
        return 'related table'
    
    def _infer_content_type(self, column_name: str, type_context: str) -> str:
        """
        Infer content type from column name and context
        """
        name_lower = column_name.lower()
        
        if any(word in name_lower for word in ['name', 'title', 'label']):
            return 'identification information'
        elif any(word in name_lower for word in ['desc', 'comment', 'note']):
            return 'descriptive text'
        elif any(word in name_lower for word in ['code', 'key', 'ref']):
            return 'reference codes'
        elif any(word in name_lower for word in ['url', 'link']):
            return 'web links'
        else:
            return type_context or 'business data'
    
    def _classify_domain(self, table_name: str, column_name: str, 
                        sample_values: List[str] = None) -> str:
        """
        Classify business domain based on table and column names
        """
        combined_text = f"{table_name} {column_name}".lower()
        
        domain_scores = {}
        for domain, keywords in self.domain_keywords.items():
            score = sum(1 for keyword in keywords if keyword in combined_text)
            if score > 0:
                domain_scores[domain] = score
        
        if domain_scores:
            return max(domain_scores, key=domain_scores.get)
        
        return 'general'
