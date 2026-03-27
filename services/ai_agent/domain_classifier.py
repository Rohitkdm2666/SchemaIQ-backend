"""
Universal Domain Classification Engine
Intelligent business domain detection for any database schema
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import re

@dataclass
class DomainAnalysis:
    primary_domain: str
    confidence: float
    secondary_domains: List[Tuple[str, float]]
    business_type: str
    industry_vertical: str
    complexity_score: float

class UniversalDomainClassifier:
    """
    Advanced domain classification system that can identify ANY business vertical
    """
    
    def __init__(self):
        self.domain_signatures = self._build_domain_signatures()
        self.industry_patterns = self._build_industry_patterns()
        self.business_type_indicators = self._build_business_type_indicators()
        
    def _build_domain_signatures(self) -> Dict[str, Dict]:
        """
        Comprehensive domain signatures with weighted keywords
        """
        return {
            'ecommerce': {
                'primary_keywords': {
                    'order': 10, 'product': 10, 'customer': 8, 'payment': 9, 'cart': 8,
                    'checkout': 8, 'shipping': 7, 'inventory': 7, 'catalog': 6, 'purchase': 9,
                    'sale': 8, 'discount': 6, 'coupon': 6, 'review': 5, 'rating': 5,
                    'wishlist': 6, 'category': 6, 'brand': 6, 'vendor': 6, 'marketplace': 8
                },
                'secondary_keywords': {
                    'sku': 5, 'barcode': 4, 'price': 4, 'stock': 4, 'warehouse': 4,
                    'fulfillment': 5, 'return': 4, 'refund': 4, 'promotion': 4
                },
                'business_type': 'B2C Retail',
                'industry': 'Retail & E-commerce'
            },
            
            'finance': {
                'primary_keywords': {
                    'account': 10, 'transaction': 10, 'balance': 9, 'payment': 8, 'loan': 8,
                    'credit': 8, 'debit': 8, 'investment': 9, 'portfolio': 8, 'bank': 9,
                    'currency': 7, 'exchange': 7, 'interest': 7, 'dividend': 6, 'bond': 6,
                    'stock': 7, 'fund': 7, 'asset': 7, 'liability': 7, 'equity': 6
                },
                'secondary_keywords': {
                    'ledger': 5, 'journal': 4, 'reconciliation': 5, 'audit': 4, 'compliance': 4,
                    'risk': 4, 'collateral': 4, 'mortgage': 5, 'insurance': 4
                },
                'business_type': 'Financial Services',
                'industry': 'Banking & Finance'
            },
            
            'healthcare': {
                'primary_keywords': {
                    'patient': 10, 'doctor': 9, 'medical': 9, 'diagnosis': 8, 'treatment': 8,
                    'prescription': 8, 'hospital': 9, 'clinic': 8, 'appointment': 7, 'insurance': 6,
                    'vital': 7, 'symptom': 7, 'medication': 8, 'drug': 7, 'therapy': 6,
                    'surgery': 7, 'nurse': 6, 'physician': 7, 'specialist': 6
                },
                'secondary_keywords': {
                    'medical_record': 5, 'chart': 4, 'lab': 4, 'test': 4, 'result': 4,
                    'radiology': 4, 'pathology': 4, 'emergency': 4, 'admission': 4
                },
                'business_type': 'Healthcare Provider',
                'industry': 'Healthcare & Medical'
            },
            
            'education': {
                'primary_keywords': {
                    'student': 10, 'teacher': 9, 'course': 9, 'class': 8, 'grade': 8,
                    'exam': 8, 'assignment': 7, 'school': 9, 'university': 8, 'enrollment': 7,
                    'curriculum': 7, 'academic': 7, 'semester': 6, 'faculty': 7, 'department': 6,
                    'degree': 7, 'major': 6, 'transcript': 6, 'tuition': 6
                },
                'secondary_keywords': {
                    'classroom': 4, 'schedule': 4, 'attendance': 4, 'library': 4, 'research': 4,
                    'thesis': 4, 'dissertation': 4, 'scholarship': 4, 'financial_aid': 4
                },
                'business_type': 'Educational Institution',
                'industry': 'Education & Training'
            },
            
            'hr_management': {
                'primary_keywords': {
                    'employee': 10, 'staff': 8, 'department': 7, 'salary': 9, 'payroll': 9,
                    'benefit': 8, 'performance': 7, 'attendance': 7, 'leave': 7, 'recruitment': 8,
                    'training': 7, 'position': 6, 'job': 6, 'role': 6, 'manager': 6,
                    'supervisor': 6, 'team': 5, 'organization': 5
                },
                'secondary_keywords': {
                    'onboarding': 4, 'offboarding': 4, 'evaluation': 4, 'promotion': 4,
                    'disciplinary': 4, 'policy': 4, 'handbook': 4, 'compliance': 4
                },
                'business_type': 'Human Resources',
                'industry': 'Corporate Services'
            },
            
            'crm': {
                'primary_keywords': {
                    'lead': 10, 'prospect': 9, 'contact': 9, 'opportunity': 9, 'deal': 9,
                    'pipeline': 8, 'campaign': 8, 'marketing': 7, 'sales': 9, 'customer': 8,
                    'relationship': 7, 'account': 6, 'territory': 6, 'quota': 6, 'commission': 6,
                    'forecast': 6, 'funnel': 7, 'conversion': 6
                },
                'secondary_keywords': {
                    'follow_up': 4, 'meeting': 4, 'call': 4, 'email': 4, 'activity': 4,
                    'task': 4, 'reminder': 4, 'note': 4, 'interaction': 4
                },
                'business_type': 'Sales & Marketing',
                'industry': 'Customer Relationship Management'
            },
            
            'manufacturing': {
                'primary_keywords': {
                    'production': 10, 'factory': 9, 'machine': 8, 'assembly': 8, 'quality': 8,
                    'batch': 8, 'material': 9, 'component': 8, 'supplier': 8, 'warehouse': 7,
                    'logistics': 7, 'inventory': 7, 'part': 7, 'process': 6, 'operation': 6,
                    'maintenance': 6, 'equipment': 7, 'tool': 5, 'shift': 5
                },
                'secondary_keywords': {
                    'blueprint': 4, 'specification': 4, 'tolerance': 4, 'inspection': 4,
                    'defect': 4, 'rework': 4, 'scrap': 4, 'yield': 4, 'efficiency': 4
                },
                'business_type': 'Manufacturing',
                'industry': 'Industrial & Manufacturing'
            },
            
            'real_estate': {
                'primary_keywords': {
                    'property': 10, 'building': 9, 'apartment': 8, 'lease': 9, 'rent': 9,
                    'tenant': 8, 'landlord': 8, 'mortgage': 8, 'listing': 8, 'agent': 8,
                    'commission': 7, 'sale': 7, 'purchase': 7, 'market': 6, 'valuation': 6,
                    'appraisal': 6, 'inspection': 6, 'closing': 6
                },
                'secondary_keywords': {
                    'square_feet': 4, 'bedroom': 4, 'bathroom': 4, 'garage': 4, 'yard': 4,
                    'neighborhood': 4, 'school_district': 4, 'amenity': 4, 'hoa': 4
                },
                'business_type': 'Real Estate',
                'industry': 'Real Estate & Property'
            },
            
            'logistics': {
                'primary_keywords': {
                    'shipment': 10, 'delivery': 10, 'carrier': 9, 'tracking': 9, 'warehouse': 8,
                    'route': 8, 'vehicle': 8, 'driver': 8, 'freight': 8, 'cargo': 8,
                    'manifest': 7, 'loading': 6, 'unloading': 6, 'dispatch': 6, 'schedule': 6,
                    'transportation': 7, 'distribution': 7, 'supply_chain': 8
                },
                'secondary_keywords': {
                    'dock': 4, 'terminal': 4, 'container': 4, 'pallet': 4, 'weight': 4,
                    'dimension': 4, 'fuel': 4, 'mileage': 4, 'maintenance': 4
                },
                'business_type': 'Logistics & Transportation',
                'industry': 'Transportation & Logistics'
            },
            
            'gaming': {
                'primary_keywords': {
                    'player': 10, 'game': 10, 'level': 9, 'achievement': 8, 'score': 9,
                    'leaderboard': 8, 'tournament': 8, 'match': 8, 'character': 8, 'item': 7,
                    'guild': 7, 'clan': 6, 'quest': 7, 'mission': 6, 'reward': 6,
                    'experience': 6, 'skill': 6, 'ability': 5, 'power': 5
                },
                'secondary_keywords': {
                    'inventory': 4, 'equipment': 4, 'weapon': 4, 'armor': 4, 'spell': 4,
                    'magic': 4, 'battle': 4, 'combat': 4, 'pvp': 4, 'pve': 4
                },
                'business_type': 'Gaming Platform',
                'industry': 'Entertainment & Gaming'
            },
            
            'social_media': {
                'primary_keywords': {
                    'user': 10, 'post': 10, 'comment': 9, 'like': 9, 'share': 9,
                    'follow': 9, 'friend': 8, 'message': 8, 'notification': 8, 'feed': 8,
                    'timeline': 7, 'profile': 8, 'photo': 6, 'video': 6, 'story': 6,
                    'hashtag': 6, 'mention': 6, 'tag': 6, 'group': 6
                },
                'secondary_keywords': {
                    'privacy': 4, 'setting': 4, 'block': 4, 'report': 4, 'moderate': 4,
                    'content': 4, 'media': 4, 'upload': 4, 'stream': 4
                },
                'business_type': 'Social Platform',
                'industry': 'Social Media & Communication'
            },
            
            'legal': {
                'primary_keywords': {
                    'case': 10, 'client': 9, 'lawyer': 9, 'attorney': 9, 'court': 9,
                    'contract': 9, 'agreement': 8, 'compliance': 8, 'regulation': 8, 'license': 7,
                    'permit': 7, 'audit': 7, 'litigation': 8, 'settlement': 7, 'judgment': 7,
                    'law': 8, 'legal': 8, 'statute': 6, 'ordinance': 6
                },
                'secondary_keywords': {
                    'brief': 4, 'motion': 4, 'hearing': 4, 'deposition': 4, 'discovery': 4,
                    'evidence': 4, 'witness': 4, 'testimony': 4, 'verdict': 4
                },
                'business_type': 'Legal Services',
                'industry': 'Legal & Professional Services'
            },
            
            'hospitality': {
                'primary_keywords': {
                    'guest': 10, 'room': 10, 'reservation': 10, 'booking': 10, 'hotel': 9,
                    'restaurant': 8, 'service': 7, 'amenity': 7, 'check_in': 9, 'check_out': 9,
                    'housekeeping': 8, 'concierge': 7, 'front_desk': 7, 'reception': 7,
                    'suite': 6, 'lobby': 6, 'dining': 6, 'menu': 6
                },
                'secondary_keywords': {
                    'occupancy': 4, 'rate': 4, 'package': 4, 'promotion': 4, 'loyalty': 4,
                    'review': 4, 'rating': 4, 'feedback': 4, 'complaint': 4
                },
                'business_type': 'Hospitality Services',
                'industry': 'Hospitality & Tourism'
            },
            
            'agriculture': {
                'primary_keywords': {
                    'farm': 10, 'crop': 10, 'livestock': 9, 'harvest': 9, 'field': 8,
                    'farmer': 8, 'irrigation': 8, 'fertilizer': 8, 'pesticide': 7, 'yield': 8,
                    'season': 7, 'planting': 7, 'growing': 6, 'soil': 6, 'weather': 6,
                    'cattle': 6, 'pig': 5, 'chicken': 5, 'dairy': 6
                },
                'secondary_keywords': {
                    'tractor': 4, 'equipment': 4, 'barn': 4, 'silo': 4, 'pasture': 4,
                    'feed': 4, 'seed': 4, 'organic': 4, 'sustainable': 4
                },
                'business_type': 'Agricultural Operations',
                'industry': 'Agriculture & Farming'
            },
            
            'energy': {
                'primary_keywords': {
                    'power': 10, 'electricity': 10, 'grid': 9, 'consumption': 8, 'generation': 9,
                    'meter': 8, 'utility': 9, 'renewable': 8, 'solar': 8, 'wind': 7,
                    'nuclear': 7, 'coal': 6, 'gas': 6, 'oil': 6, 'energy': 9,
                    'voltage': 6, 'current': 6, 'transformer': 6, 'substation': 6
                },
                'secondary_keywords': {
                    'turbine': 4, 'generator': 4, 'panel': 4, 'battery': 4, 'storage': 4,
                    'transmission': 4, 'distribution': 4, 'load': 4, 'demand': 4
                },
                'business_type': 'Energy Provider',
                'industry': 'Energy & Utilities'
            },
            
            'telecommunications': {
                'primary_keywords': {
                    'call': 10, 'phone': 9, 'network': 10, 'signal': 8, 'tower': 8,
                    'subscriber': 9, 'plan': 8, 'usage': 8, 'roaming': 7, 'bandwidth': 8,
                    'data': 8, 'voice': 8, 'sms': 7, 'mms': 6, 'internet': 7,
                    'broadband': 7, 'fiber': 6, 'wireless': 7, 'cellular': 7
                },
                'secondary_keywords': {
                    'antenna': 4, 'frequency': 4, 'spectrum': 4, 'coverage': 4, 'quality': 4,
                    'latency': 4, 'throughput': 4, 'protocol': 4, 'infrastructure': 4
                },
                'business_type': 'Telecommunications Provider',
                'industry': 'Telecommunications'
            },
            
            'insurance': {
                'primary_keywords': {
                    'policy': 10, 'claim': 10, 'premium': 9, 'coverage': 9, 'beneficiary': 8,
                    'deductible': 8, 'underwriting': 9, 'risk': 9, 'adjuster': 8, 'settlement': 8,
                    'liability': 8, 'auto': 6, 'home': 6, 'life': 6, 'health': 6,
                    'accident': 6, 'damage': 6, 'loss': 6, 'fraud': 6
                },
                'secondary_keywords': {
                    'actuarial': 4, 'reserve': 4, 'reinsurance': 4, 'agent': 4, 'broker': 4,
                    'quote': 4, 'renewal': 4, 'cancellation': 4, 'exclusion': 4
                },
                'business_type': 'Insurance Provider',
                'industry': 'Insurance & Risk Management'
            },
            
            'media': {
                'primary_keywords': {
                    'content': 10, 'article': 9, 'video': 9, 'audio': 8, 'publication': 8,
                    'author': 8, 'editor': 8, 'subscriber': 8, 'viewer': 8, 'rating': 7,
                    'channel': 8, 'broadcast': 8, 'stream': 7, 'podcast': 7, 'blog': 6,
                    'news': 7, 'magazine': 6, 'newspaper': 6, 'television': 6
                },
                'secondary_keywords': {
                    'headline': 4, 'byline': 4, 'caption': 4, 'transcript': 4, 'script': 4,
                    'production': 4, 'studio': 4, 'equipment': 4, 'archive': 4
                },
                'business_type': 'Media Company',
                'industry': 'Media & Entertainment'
            },
            
            'government': {
                'primary_keywords': {
                    'citizen': 10, 'service': 8, 'department': 9, 'office': 7, 'permit': 9,
                    'license': 8, 'tax': 9, 'regulation': 8, 'policy': 8, 'public': 7,
                    'municipal': 8, 'federal': 8, 'state': 7, 'local': 6, 'government': 9,
                    'agency': 7, 'bureau': 6, 'commission': 6, 'authority': 6
                },
                'secondary_keywords': {
                    'ordinance': 4, 'statute': 4, 'code': 4, 'compliance': 4, 'inspection': 4,
                    'violation': 4, 'fine': 4, 'penalty': 4, 'hearing': 4
                },
                'business_type': 'Government Agency',
                'industry': 'Government & Public Sector'
            }
        }
    
    def _build_industry_patterns(self) -> Dict[str, List[str]]:
        """
        Industry vertical patterns for higher-level classification
        """
        return {
            'technology': ['software', 'hardware', 'system', 'application', 'platform', 'digital', 'tech'],
            'financial_services': ['bank', 'finance', 'investment', 'trading', 'capital', 'fund', 'asset'],
            'healthcare': ['medical', 'health', 'clinical', 'pharmaceutical', 'biotech', 'wellness'],
            'retail': ['store', 'shop', 'merchant', 'retail', 'commerce', 'marketplace', 'vendor'],
            'manufacturing': ['factory', 'plant', 'production', 'assembly', 'industrial', 'manufacturing'],
            'services': ['service', 'consulting', 'professional', 'agency', 'firm', 'bureau'],
            'transportation': ['transport', 'logistics', 'shipping', 'delivery', 'freight', 'cargo'],
            'entertainment': ['media', 'entertainment', 'gaming', 'sports', 'recreation', 'leisure'],
            'education': ['school', 'university', 'college', 'academic', 'educational', 'training'],
            'government': ['government', 'public', 'municipal', 'federal', 'state', 'civic']
        }
    
    def _build_business_type_indicators(self) -> Dict[str, List[str]]:
        """
        Business model type indicators
        """
        return {
            'b2b': ['vendor', 'supplier', 'partner', 'client', 'enterprise', 'business', 'corporate'],
            'b2c': ['customer', 'consumer', 'user', 'member', 'subscriber', 'buyer', 'shopper'],
            'marketplace': ['marketplace', 'platform', 'exchange', 'hub', 'network', 'community'],
            'saas': ['subscription', 'tenant', 'workspace', 'organization', 'account', 'license'],
            'nonprofit': ['donor', 'volunteer', 'charity', 'foundation', 'nonprofit', 'ngo'],
            'government': ['citizen', 'resident', 'taxpayer', 'public', 'municipal', 'federal']
        }
    
    def classify_schema(self, tables: List[Dict], relationships: List[Dict] = None) -> DomainAnalysis:
        """
        Comprehensive domain classification of entire schema
        """
        # Extract all text for analysis
        all_text = self._extract_schema_text(tables)
        
        # Calculate domain scores
        domain_scores = self._calculate_domain_scores(all_text)
        
        # Determine primary domain
        primary_domain = max(domain_scores, key=domain_scores.get) if domain_scores else 'general'
        primary_confidence = domain_scores.get(primary_domain, 0.0)
        
        # Get secondary domains
        secondary_domains = [(domain, score) for domain, score in sorted(domain_scores.items(), key=lambda x: x[1], reverse=True)[1:4]]
        
        # Classify business type
        business_type = self._classify_business_type(all_text, primary_domain)
        
        # Determine industry vertical
        industry_vertical = self._classify_industry_vertical(all_text, primary_domain)
        
        # Calculate complexity score
        complexity_score = self._calculate_complexity_score(tables, relationships)
        
        return DomainAnalysis(
            primary_domain=primary_domain,
            confidence=primary_confidence,
            secondary_domains=secondary_domains,
            business_type=business_type,
            industry_vertical=industry_vertical,
            complexity_score=complexity_score
        )
    
    def _extract_schema_text(self, tables: List[Dict]) -> str:
        """
        Extract all textual information from schema
        """
        text_parts = []
        
        for table in tables:
            # Add table name
            text_parts.append(table.get('name', ''))
            
            # Add column names
            for column in table.get('columns', []):
                text_parts.append(column.get('name', ''))
        
        return ' '.join(text_parts).lower()
    
    def _calculate_domain_scores(self, text: str) -> Dict[str, float]:
        """
        Calculate weighted scores for each domain
        """
        domain_scores = {}
        
        for domain, signature in self.domain_signatures.items():
            score = 0.0
            total_weight = 0.0
            
            # Primary keywords
            for keyword, weight in signature['primary_keywords'].items():
                if keyword in text:
                    score += weight
                total_weight += weight
            
            # Secondary keywords
            for keyword, weight in signature['secondary_keywords'].items():
                if keyword in text:
                    score += weight
                total_weight += weight
            
            # Normalize score
            if total_weight > 0:
                domain_scores[domain] = score / total_weight
            else:
                domain_scores[domain] = 0.0
        
        return domain_scores
    
    def _classify_business_type(self, text: str, primary_domain: str) -> str:
        """
        Classify business model type
        """
        type_scores = {}
        
        for business_type, indicators in self.business_type_indicators.items():
            score = sum(1 for indicator in indicators if indicator in text)
            if score > 0:
                type_scores[business_type] = score
        
        if type_scores:
            classified_type = max(type_scores, key=type_scores.get)
            return classified_type.upper().replace('_', '2').replace('2', '2')
        
        # Fallback to domain-based classification
        domain_signature = self.domain_signatures.get(primary_domain, {})
        return domain_signature.get('business_type', 'General Business')
    
    def _classify_industry_vertical(self, text: str, primary_domain: str) -> str:
        """
        Classify industry vertical
        """
        industry_scores = {}
        
        for industry, patterns in self.industry_patterns.items():
            score = sum(1 for pattern in patterns if pattern in text)
            if score > 0:
                industry_scores[industry] = score
        
        if industry_scores:
            return max(industry_scores, key=industry_scores.get).replace('_', ' ').title()
        
        # Fallback to domain-based classification
        domain_signature = self.domain_signatures.get(primary_domain, {})
        return domain_signature.get('industry', 'General Industry')
    
    def _calculate_complexity_score(self, tables: List[Dict], relationships: List[Dict] = None) -> float:
        """
        Calculate schema complexity score (0.0 to 1.0)
        """
        if not tables:
            return 0.0
        
        # Base factors
        table_count = len(tables)
        total_columns = sum(len(table.get('columns', [])) for table in tables)
        relationship_count = len(relationships) if relationships else 0
        
        # Complexity indicators
        avg_columns_per_table = total_columns / table_count if table_count > 0 else 0
        
        # Calculate normalized complexity
        table_complexity = min(table_count / 20.0, 1.0)  # 20+ tables = max complexity
        column_complexity = min(avg_columns_per_table / 15.0, 1.0)  # 15+ avg columns = max complexity
        relationship_complexity = min(relationship_count / 30.0, 1.0)  # 30+ relationships = max complexity
        
        # Weighted average
        complexity = (table_complexity * 0.4 + column_complexity * 0.3 + relationship_complexity * 0.3)
        
        return round(complexity, 2)
    
    def get_domain_context(self, domain: str) -> Dict[str, str]:
        """
        Get contextual information about a domain
        """
        domain_signature = self.domain_signatures.get(domain, {})
        
        return {
            'business_type': domain_signature.get('business_type', 'General Business'),
            'industry': domain_signature.get('industry', 'General Industry'),
            'description': self._get_domain_description(domain),
            'common_entities': list(domain_signature.get('primary_keywords', {}).keys())[:5]
        }
    
    def _get_domain_description(self, domain: str) -> str:
        """
        Get human-readable description of domain
        """
        descriptions = {
            'ecommerce': 'Online retail and e-commerce operations focused on product sales and customer transactions',
            'finance': 'Financial services including banking, investments, and monetary transactions',
            'healthcare': 'Medical and healthcare services including patient care and clinical operations',
            'education': 'Educational institutions and academic management systems',
            'hr_management': 'Human resources and employee management operations',
            'crm': 'Customer relationship management and sales operations',
            'manufacturing': 'Industrial manufacturing and production operations',
            'real_estate': 'Property management and real estate transactions',
            'logistics': 'Transportation, shipping, and supply chain management',
            'gaming': 'Gaming platforms and entertainment systems',
            'social_media': 'Social networking and communication platforms',
            'legal': 'Legal services and compliance management',
            'hospitality': 'Hotel, restaurant, and hospitality services',
            'agriculture': 'Agricultural operations and farming management',
            'energy': 'Energy production and utility services',
            'telecommunications': 'Communication services and network operations',
            'insurance': 'Insurance services and risk management',
            'media': 'Media production and content management',
            'government': 'Government services and public administration'
        }
        
        return descriptions.get(domain, 'General business operations and data management')
