"""
Intelligent Context Generation Engine
Advanced business context and description generation for database schemas
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import re
from .universal_patterns import UniversalPatternEngine, ColumnContext
from .domain_classifier import UniversalDomainClassifier, DomainAnalysis

@dataclass
class TableContext:
    name: str
    description: str
    business_purpose: str
    domain_role: str
    data_category: str
    relationships_summary: str

@dataclass
class SchemaContext:
    tables: List[TableContext]
    columns: Dict[str, List[ColumnContext]]  # table_name -> column_contexts
    domain_analysis: DomainAnalysis
    business_summary: str
    technical_summary: str

class IntelligentContextGenerator:
    """
    Advanced AI agent for generating comprehensive business context
    """
    
    def __init__(self):
        self.pattern_engine = UniversalPatternEngine()
        self.domain_classifier = UniversalDomainClassifier()
        self.table_templates = self._build_table_templates()
        self.business_workflows = self._build_business_workflows()
        
    def _build_table_templates(self) -> Dict[str, Dict]:
        """
        Comprehensive table description templates by domain
        """
        return {
            'ecommerce': {
                'customer': {
                    'desc': 'Customer information and contact details',
                    'purpose': 'Stores customer profiles, contact information, and account details for order processing and customer service',
                    'role': 'Core entity representing buyers and account holders in the e-commerce system'
                },
                'order': {
                    'desc': 'Purchase transactions and order lifecycle management',
                    'purpose': 'Tracks customer purchases from placement through fulfillment, including order status and processing workflow',
                    'role': 'Central transaction entity connecting customers, products, and business operations'
                },
                'product': {
                    'desc': 'Product catalog with pricing and inventory details',
                    'purpose': 'Maintains product information, pricing, descriptions, and inventory levels for sales operations',
                    'role': 'Inventory master data driving sales, pricing, and catalog management'
                },
                'payment': {
                    'desc': 'Financial transaction records and payment processing',
                    'purpose': 'Records payment transactions, methods, and status for financial reconciliation and accounting',
                    'role': 'Financial processing entity ensuring secure and tracked monetary transactions'
                },
                'shipping': {
                    'desc': 'Delivery and logistics tracking information',
                    'purpose': 'Manages shipping addresses, delivery status, and logistics coordination for order fulfillment',
                    'role': 'Logistics coordination entity ensuring successful product delivery'
                }
            },
            'finance': {
                'account': {
                    'desc': 'Financial account information and balances',
                    'purpose': 'Maintains account details, balances, and account holder information for banking operations',
                    'role': 'Core financial entity representing customer financial relationships'
                },
                'transaction': {
                    'desc': 'Financial movements and transaction history',
                    'purpose': 'Records all financial transactions including debits, credits, and transfers for audit and reporting',
                    'role': 'Transaction ledger ensuring accurate financial record keeping and compliance'
                },
                'loan': {
                    'desc': 'Loan agreements and repayment tracking',
                    'purpose': 'Manages loan terms, balances, payments, and compliance for lending operations',
                    'role': 'Credit management entity tracking lending relationships and obligations'
                },
                'investment': {
                    'desc': 'Investment portfolio and asset management',
                    'purpose': 'Tracks investment holdings, performance, and portfolio allocation for wealth management',
                    'role': 'Asset management entity supporting investment advisory and portfolio services'
                }
            },
            'healthcare': {
                'patient': {
                    'desc': 'Patient information and medical records',
                    'purpose': 'Stores patient demographics, medical history, and contact information for healthcare delivery',
                    'role': 'Central healthcare entity representing individuals receiving medical care'
                },
                'appointment': {
                    'desc': 'Medical appointments and scheduling',
                    'purpose': 'Manages healthcare appointments, scheduling, and provider availability for patient care coordination',
                    'role': 'Scheduling entity ensuring efficient healthcare resource utilization'
                },
                'prescription': {
                    'desc': 'Medication prescriptions and pharmacy orders',
                    'purpose': 'Tracks prescribed medications, dosages, and pharmacy fulfillment for patient safety',
                    'role': 'Medication management entity ensuring safe and effective drug therapy'
                },
                'diagnosis': {
                    'desc': 'Medical diagnoses and clinical assessments',
                    'purpose': 'Records medical diagnoses, conditions, and clinical findings for treatment planning',
                    'role': 'Clinical documentation entity supporting evidence-based medical care'
                }
            },
            'education': {
                'student': {
                    'desc': 'Student enrollment and academic records',
                    'purpose': 'Maintains student information, enrollment status, and academic progress for educational administration',
                    'role': 'Core academic entity representing learners in the educational system'
                },
                'course': {
                    'desc': 'Academic courses and curriculum management',
                    'purpose': 'Defines course content, requirements, and scheduling for academic program delivery',
                    'role': 'Curriculum entity structuring educational content and learning objectives'
                },
                'grade': {
                    'desc': 'Academic performance and assessment records',
                    'purpose': 'Tracks student performance, grades, and academic achievements for progress monitoring',
                    'role': 'Assessment entity measuring and recording educational outcomes'
                },
                'enrollment': {
                    'desc': 'Course registration and enrollment tracking',
                    'purpose': 'Manages student course registrations, capacity, and enrollment status for academic planning',
                    'role': 'Registration entity coordinating student-course relationships'
                }
            },
            'hr_management': {
                'employee': {
                    'desc': 'Employee information and HR records',
                    'purpose': 'Stores employee profiles, job information, and HR data for workforce management',
                    'role': 'Core HR entity representing organizational human resources'
                },
                'payroll': {
                    'desc': 'Salary and compensation processing',
                    'purpose': 'Manages employee compensation, benefits, and payroll processing for HR operations',
                    'role': 'Compensation entity ensuring accurate and timely employee payments'
                },
                'performance': {
                    'desc': 'Employee performance evaluations and reviews',
                    'purpose': 'Tracks employee performance metrics, reviews, and development goals for talent management',
                    'role': 'Performance management entity supporting employee development and evaluation'
                },
                'department': {
                    'desc': 'Organizational structure and department management',
                    'purpose': 'Defines organizational units, reporting structures, and departmental information',
                    'role': 'Organizational entity structuring workforce hierarchy and reporting relationships'
                }
            }
        }
    
    def _build_business_workflows(self) -> Dict[str, Dict]:
        """
        Business workflow patterns for relationship context
        """
        return {
            'ecommerce': {
                'order_flow': 'Customer places order → Payment processing → Inventory allocation → Shipping → Delivery',
                'customer_journey': 'Registration → Browse products → Add to cart → Checkout → Payment → Order tracking',
                'inventory_flow': 'Product creation → Stock management → Order fulfillment → Inventory updates'
            },
            'finance': {
                'transaction_flow': 'Account verification → Transaction authorization → Processing → Settlement → Reconciliation',
                'lending_flow': 'Application → Credit check → Approval → Loan disbursement → Repayment tracking',
                'investment_flow': 'Account opening → Risk assessment → Investment selection → Portfolio management → Reporting'
            },
            'healthcare': {
                'patient_flow': 'Registration → Appointment scheduling → Clinical assessment → Treatment → Follow-up',
                'treatment_flow': 'Diagnosis → Treatment planning → Prescription → Monitoring → Outcome assessment',
                'clinical_flow': 'Patient intake → Medical history → Examination → Diagnosis → Treatment plan'
            },
            'education': {
                'academic_flow': 'Student enrollment → Course registration → Attendance → Assessment → Grading → Graduation',
                'learning_flow': 'Course planning → Content delivery → Student engagement → Assessment → Progress tracking',
                'administrative_flow': 'Registration → Fee payment → Course allocation → Academic tracking → Certification'
            }
        }
    
    def generate_comprehensive_context(self, tables: List[Dict], relationships: List[Dict] = None,
                                     sample_data: Dict[str, Dict] = None) -> SchemaContext:
        """
        Generate comprehensive context for entire schema
        """
        # Classify domain
        domain_analysis = self.domain_classifier.classify_schema(tables, relationships)
        
        # Generate table contexts
        table_contexts = []
        column_contexts = {}
        
        for table in tables:
            table_name = table.get('name', '')
            
            # Generate table context
            table_context = self._generate_table_context(table, domain_analysis, relationships)
            table_contexts.append(table_context)
            
            # Generate column contexts
            columns = table.get('columns', [])
            table_samples = sample_data.get(table_name, {}) if sample_data else {}
            
            column_list = []
            for column in columns:
                column_name = column.get('name', '')
                column_type = column.get('type', '')
                samples = table_samples.get(column_name, [])
                
                column_context = self.pattern_engine.analyze_column(
                    column_name, column_type, table_name, samples, relationships
                )
                column_list.append(column_context)
            
            column_contexts[table_name] = column_list
        
        # Generate business and technical summaries
        business_summary = self._generate_business_summary(domain_analysis, table_contexts, relationships)
        technical_summary = self._generate_technical_summary(tables, relationships, domain_analysis)
        
        return SchemaContext(
            tables=table_contexts,
            columns=column_contexts,
            domain_analysis=domain_analysis,
            business_summary=business_summary,
            technical_summary=technical_summary
        )
    
    def _generate_table_context(self, table: Dict, domain_analysis: DomainAnalysis, 
                               relationships: List[Dict] = None) -> TableContext:
        """
        Generate intelligent context for a table
        """
        table_name = table.get('name', '').lower()
        primary_domain = domain_analysis.primary_domain
        
        # Get domain-specific template
        domain_templates = self.table_templates.get(primary_domain, {})
        
        # Find best matching template
        best_template = None
        for template_key, template_data in domain_templates.items():
            if template_key in table_name or any(word in table_name for word in template_key.split('_')):
                best_template = template_data
                break
        
        # Generate context
        if best_template:
            description = best_template['desc']
            business_purpose = best_template['purpose']
            domain_role = best_template['role']
        else:
            description = self._generate_generic_table_description(table_name, table)
            business_purpose = self._generate_generic_business_purpose(table_name, domain_analysis)
            domain_role = self._generate_generic_domain_role(table_name, domain_analysis)
        
        # Analyze relationships
        relationships_summary = self._generate_relationships_summary(table_name, relationships)
        
        # Determine data category
        data_category = self._classify_table_category(table_name, table)
        
        return TableContext(
            name=table.get('name', ''),
            description=description,
            business_purpose=business_purpose,
            domain_role=domain_role,
            data_category=data_category,
            relationships_summary=relationships_summary
        )
    
    def _generate_generic_table_description(self, table_name: str, table: Dict) -> str:
        """
        Generate generic table description using intelligent inference
        """
        # Analyze table name patterns
        name_lower = table_name.lower()
        
        # Common table type patterns
        if any(word in name_lower for word in ['user', 'customer', 'client', 'member']):
            return f"{table_name.title()} profiles and account information"
        elif any(word in name_lower for word in ['order', 'transaction', 'purchase', 'sale']):
            return f"{table_name.title()} records and transaction history"
        elif any(word in name_lower for word in ['product', 'item', 'inventory', 'catalog']):
            return f"{table_name.title()} information and catalog data"
        elif any(word in name_lower for word in ['payment', 'billing', 'invoice']):
            return f"{table_name.title()} processing and financial records"
        elif any(word in name_lower for word in ['log', 'audit', 'history']):
            return f"{table_name.title()} tracking and audit trail information"
        elif any(word in name_lower for word in ['config', 'setting', 'preference']):
            return f"{table_name.title()} configuration and system preferences"
        elif any(word in name_lower for word in ['report', 'analytics', 'metric']):
            return f"{table_name.title()} data and performance metrics"
        else:
            return f"{table_name.title()} data and related information"
    
    def _generate_generic_business_purpose(self, table_name: str, domain_analysis: DomainAnalysis) -> str:
        """
        Generate generic business purpose based on domain context
        """
        domain_context = self.domain_classifier.get_domain_context(domain_analysis.primary_domain)
        business_type = domain_context.get('business_type', 'business operations')
        
        name_lower = table_name.lower()
        
        if any(word in name_lower for word in ['user', 'customer', 'client']):
            return f"Manages customer relationships and account information critical for {business_type.lower()} and service delivery"
        elif any(word in name_lower for word in ['order', 'transaction']):
            return f"Processes and tracks business transactions essential for revenue generation and {business_type.lower()}"
        elif any(word in name_lower for word in ['product', 'inventory']):
            return f"Maintains product catalog and inventory data supporting sales operations and {business_type.lower()}"
        else:
            return f"Supports {business_type.lower()} through structured data management and operational processes"
    
    def _generate_generic_domain_role(self, table_name: str, domain_analysis: DomainAnalysis) -> str:
        """
        Generate domain-specific role description
        """
        name_lower = table_name.lower()
        
        if any(word in name_lower for word in ['user', 'customer', 'client', 'member']):
            return "Core entity representing system users and their associated data"
        elif any(word in name_lower for word in ['order', 'transaction', 'purchase']):
            return "Transaction entity capturing business events and financial activities"
        elif any(word in name_lower for word in ['product', 'item', 'inventory']):
            return "Master data entity defining business offerings and resources"
        elif any(word in name_lower for word in ['log', 'audit', 'history']):
            return "Audit entity providing traceability and compliance support"
        else:
            return "Supporting entity contributing to overall system functionality and data integrity"
    
    def _generate_relationships_summary(self, table_name: str, relationships: List[Dict] = None) -> str:
        """
        Generate summary of table relationships
        """
        if not relationships:
            return "No explicit relationships defined"
        
        # Find relationships involving this table
        incoming = []
        outgoing = []
        
        for rel in relationships:
            if rel.get('to_table', '').lower() == table_name.lower():
                incoming.append(rel.get('from_table', ''))
            elif rel.get('from_table', '').lower() == table_name.lower():
                outgoing.append(rel.get('to_table', ''))
        
        summary_parts = []
        
        if incoming:
            summary_parts.append(f"Referenced by: {', '.join(incoming)}")
        
        if outgoing:
            summary_parts.append(f"References: {', '.join(outgoing)}")
        
        if not summary_parts:
            return "No direct relationships identified"
        
        return "; ".join(summary_parts)
    
    def _classify_table_category(self, table_name: str, table: Dict) -> str:
        """
        Classify table into data category
        """
        name_lower = table_name.lower()
        
        # Master data
        if any(word in name_lower for word in ['customer', 'product', 'user', 'employee', 'account']):
            return 'master_data'
        
        # Transaction data
        elif any(word in name_lower for word in ['order', 'transaction', 'payment', 'sale', 'purchase']):
            return 'transactional_data'
        
        # Reference data
        elif any(word in name_lower for word in ['category', 'type', 'status', 'code', 'lookup']):
            return 'reference_data'
        
        # Audit/Log data
        elif any(word in name_lower for word in ['log', 'audit', 'history', 'track']):
            return 'audit_data'
        
        # Configuration data
        elif any(word in name_lower for word in ['config', 'setting', 'preference', 'parameter']):
            return 'configuration_data'
        
        # Analytical data
        elif any(word in name_lower for word in ['report', 'analytics', 'metric', 'summary']):
            return 'analytical_data'
        
        else:
            return 'operational_data'
    
    def _generate_business_summary(self, domain_analysis: DomainAnalysis, 
                                 table_contexts: List[TableContext], 
                                 relationships: List[Dict] = None) -> str:
        """
        Generate comprehensive business summary
        """
        domain_info = self.domain_classifier.get_domain_context(domain_analysis.primary_domain)
        
        # Count table categories
        category_counts = {}
        for table_context in table_contexts:
            category = table_context.data_category
            category_counts[category] = category_counts.get(category, 0) + 1
        
        # Build summary
        summary_parts = [
            f"This database supports {domain_info['business_type']} operations in the {domain_info['industry']} sector.",
            f"The schema contains {len(table_contexts)} tables with {domain_analysis.complexity_score:.0%} complexity.",
        ]
        
        # Add workflow context
        workflows = self.business_workflows.get(domain_analysis.primary_domain, {})
        if workflows:
            primary_workflow = list(workflows.values())[0]
            summary_parts.append(f"Primary business workflow: {primary_workflow}")
        
        # Add relationship context
        if relationships:
            summary_parts.append(f"Data relationships support integrated business processes across {len(relationships)} connections.")
        
        return " ".join(summary_parts)
    
    def _generate_technical_summary(self, tables: List[Dict], relationships: List[Dict] = None,
                                  domain_analysis: DomainAnalysis = None) -> str:
        """
        Generate technical architecture summary
        """
        total_columns = sum(len(table.get('columns', [])) for table in tables)
        avg_columns = total_columns / len(tables) if tables else 0
        
        # Analyze data types
        type_counts = {}
        for table in tables:
            for column in table.get('columns', []):
                col_type = column.get('type', 'unknown').lower()
                type_counts[col_type] = type_counts.get(col_type, 0) + 1
        
        summary_parts = [
            f"Database schema with {len(tables)} tables and {total_columns} total columns.",
            f"Average of {avg_columns:.1f} columns per table indicating {'high' if avg_columns > 10 else 'moderate'} normalization.",
        ]
        
        # Most common data types
        if type_counts:
            top_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:3]
            type_summary = ", ".join([f"{count} {dtype}" for dtype, count in top_types])
            summary_parts.append(f"Primary data types: {type_summary}.")
        
        # Relationship complexity
        if relationships:
            summary_parts.append(f"Relational integrity maintained through {len(relationships)} foreign key relationships.")
        
        return " ".join(summary_parts)
