#!/usr/bin/env python3
"""
Advanced Address Matching Analysis Tool for ZabaSearch System
Analyzes the 10% failure rate in address matching and provides diagnostic insights
"""

import re
import pandas as pd
from typing import Dict, List, Tuple, Optional
import json
from difflib import SequenceMatcher
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AddressMatchingAnalyzer:
    def __init__(self):
        self.failure_patterns = []
        self.success_patterns = []
        self.edge_cases = []
        
    def normalize_address(self, address: str) -> str:
        """Enhanced address normalization with diagnostic tracking"""
        if not address:
            return ""

        original = address
        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', address.upper().strip())

        # Common address normalizations
        replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE', 
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' PLACE': ' PL',
            ' ROAD': ' RD',
            ' CIRCLE': ' CIR',
            ' WAY': ' WAY',
            ' BOULEVARD': ' BLVD',
            ' LANE': ' LN',
            ' TERRACE': ' TER'
        }

        for old, new in replacements.items():
            normalized = normalized.replace(old, new)

        # Track transformation for analysis
        transformation = {
            'original': original,
            'normalized': normalized,
            'replacements_applied': []
        }
        
        for old, new in replacements.items():
            if old in address.upper():
                transformation['replacements_applied'].append(f"{old} -> {new}")
                
        return normalized
    
    def analyze_address_components(self, address: str) -> Dict:
        """Break down address into components for detailed analysis"""
        components = {
            'street_number': None,
            'street_name': [],
            'street_type': None,
            'directional': [],
            'unit': None,
            'city': None,
            'state': None,
            'zip': None,
            'ordinals': [],
            'special_chars': []
        }
        
        if not address:
            return components
            
        normalized = self.normalize_address(address)
        parts = normalized.split()
        
        # Extract street number (first numeric part)
        if parts and re.match(r'^\d+', parts[0]):
            components['street_number'] = parts[0]
            
        # Find ordinal numbers (1ST, 2ND, 3RD, etc.)
        ordinal_pattern = r'\d+(ST|ND|RD|TH)\b'
        components['ordinals'] = re.findall(ordinal_pattern, normalized)
        
        # Find directional indicators
        directionals = ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'SOUTH', 'EAST', 'WEST']
        for part in parts:
            if part in directionals:
                components['directional'].append(part)
                
        # Find street types
        street_types = ['ST', 'AVE', 'DR', 'CT', 'PL', 'RD', 'CIR', 'WAY', 'BLVD', 'LN', 'TER']
        for part in parts:
            if part in street_types:
                components['street_type'] = part
                break
                
        # Find special characters and formatting
        special_chars = re.findall(r'[^\w\s]', address)
        components['special_chars'] = list(set(special_chars))
        
        # Identify potential problem areas
        components['analysis_flags'] = []
        
        if len(components['ordinals']) > 1:
            components['analysis_flags'].append('MULTIPLE_ORDINALS')
            
        if len(components['directional']) > 2:
            components['analysis_flags'].append('EXCESSIVE_DIRECTIONALS')
            
        if not components['street_number']:
            components['analysis_flags'].append('NO_STREET_NUMBER')
            
        if len(special_chars) > 3:
            components['analysis_flags'].append('COMPLEX_FORMATTING')
            
        return components
    
    def simulate_addresses_match(self, csv_address: str, zaba_address: str) -> Dict:
        """Simulate the current address matching logic with detailed analysis"""
        result = {
            'match': False,
            'csv_normalized': '',
            'zaba_normalized': '',
            'csv_components': {},
            'zaba_components': {},
            'comparison_details': {},
            'failure_reasons': [],
            'similarity_score': 0.0,
            'debug_info': []
        }
        
        if not csv_address or not zaba_address:
            result['failure_reasons'].append('EMPTY_ADDRESS')
            return result

        # Normalize both addresses
        csv_norm = self.normalize_address(csv_address)
        zaba_norm = self.normalize_address(zaba_address)
        
        result['csv_normalized'] = csv_norm
        result['zaba_normalized'] = zaba_norm
        
        # Analyze components
        result['csv_components'] = self.analyze_address_components(csv_address)
        result['zaba_components'] = self.analyze_address_components(zaba_address)
        
        # Calculate similarity score
        result['similarity_score'] = SequenceMatcher(None, csv_norm, zaba_norm).ratio()
        
        result['debug_info'].append(f"Comparing: '{csv_norm}' vs '{zaba_norm}'")

        # Extract components for flexible matching (current logic)
        csv_parts = csv_norm.split()
        zaba_parts = zaba_norm.split()

        if len(csv_parts) < 2 or len(zaba_parts) < 2:
            result['failure_reasons'].append('INSUFFICIENT_PARTS')
            return result

        # Check if street number matches (current logic)
        if csv_parts[0] != zaba_parts[0]:
            result['failure_reasons'].append('STREET_NUMBER_MISMATCH')
            result['debug_info'].append(f"Street number mismatch: '{csv_parts[0]}' vs '{zaba_parts[0]}'")
            return result

        # Simulate ordinal variations creation
        def create_ordinal_variations(parts):
            variations = []
            for part in parts:
                variations.append(part)
                if re.match(r'^\d+$', part):
                    num = int(part)
                    if num == 1:
                        variations.extend([f"{part}ST", "1ST"])
                    elif num == 2:
                        variations.extend([f"{part}ND", "2ND"])
                    elif num == 3:
                        variations.extend([f"{part}RD", "3RD"])
                    elif num in [11, 12, 13]:
                        variations.append(f"{part}TH")
                    elif num % 10 == 1:
                        variations.append(f"{part}ST")
                    elif num % 10 == 2:
                        variations.append(f"{part}ND")
                    elif num % 10 == 3:
                        variations.append(f"{part}RD")
                    else:
                        variations.append(f"{part}TH")
                elif re.match(r'^\d+(ST|ND|RD|TH)$', part):
                    base_num = re.sub(r'(ST|ND|RD|TH)$', '', part)
                    variations.append(base_num)
            return variations

        # Get key street parts (current logic)
        csv_street_parts = csv_parts[1:4] if len(csv_parts) > 3 else csv_parts[1:]
        zaba_street_parts = zaba_parts[1:4] if len(zaba_parts) > 3 else zaba_parts[1:]

        # Create variations
        csv_variations = create_ordinal_variations(csv_street_parts)
        zaba_variations = create_ordinal_variations(zaba_street_parts)
        
        result['comparison_details'] = {
            'csv_street_parts': csv_street_parts,
            'zaba_street_parts': zaba_street_parts,
            'csv_variations': csv_variations,
            'zaba_variations': zaba_variations
        }

        # Count matches
        matches = 0
        matched_parts = []

        for csv_var in csv_variations:
            if csv_var in zaba_variations and csv_var not in matched_parts:
                matches += 1
                matched_parts.append(csv_var)
                if matches >= 2:
                    break

        result['comparison_details']['matches'] = matches
        result['comparison_details']['matched_parts'] = matched_parts
        
        # Current logic requires at least 2 matching parts
        result['match'] = matches >= 2
        
        if not result['match']:
            if matches == 0:
                result['failure_reasons'].append('NO_MATCHING_PARTS')
            elif matches == 1:
                result['failure_reasons'].append('INSUFFICIENT_MATCHES_ONLY_ONE')
                
        result['debug_info'].append(f"Found {matches} matching parts {matched_parts}, result: {'✅' if result['match'] else '❌'}")
        
        return result
    
    def identify_failure_patterns(self, test_cases: List[Tuple[str, str, bool]]) -> Dict:
        """Analyze multiple test cases to identify failure patterns"""
        patterns = {
            'ordinal_issues': 0,
            'directional_issues': 0,
            'abbreviation_issues': 0,
            'spacing_issues': 0,
            'case_sensitivity_issues': 0,
            'partial_address_issues': 0,
            'format_inconsistency': 0,
            'false_negatives': [],
            'false_positives': [],
            'edge_cases': []
        }
        
        for csv_addr, zaba_addr, expected_match in test_cases:
            result = self.simulate_addresses_match(csv_addr, zaba_addr)
            
            # Check for false negatives (should match but doesn't)
            if expected_match and not result['match']:
                patterns['false_negatives'].append({
                    'csv': csv_addr,
                    'zaba': zaba_addr,
                    'analysis': result
                })
                
                # Categorize the failure
                if 'ORDINAL' in ' '.join(result['failure_reasons']):
                    patterns['ordinal_issues'] += 1
                if any('directional' in flag.lower() for flag in result['csv_components'].get('analysis_flags', [])):
                    patterns['directional_issues'] += 1
                    
            # Check for false positives (shouldn't match but does)
            elif not expected_match and result['match']:
                patterns['false_positives'].append({
                    'csv': csv_addr,
                    'zaba': zaba_addr,
                    'analysis': result
                })
                
            # Flag edge cases (low similarity but matched, or high similarity but didn't match)
            if (result['match'] and result['similarity_score'] < 0.6) or \
               (not result['match'] and result['similarity_score'] > 0.8):
                patterns['edge_cases'].append({
                    'csv': csv_addr,
                    'zaba': zaba_addr,
                    'similarity': result['similarity_score'],
                    'matched': result['match'],
                    'analysis': result
                })
        
        return patterns
    
    def generate_test_cases(self) -> List[Tuple[str, str, bool]]:
        """Generate comprehensive test cases for address matching"""
        test_cases = [
            # Basic matches that should work
            ("123 MAIN ST", "123 MAIN ST", True),
            ("456 OAK AVE", "456 OAK AVENUE", True),
            ("789 FIRST DR", "789 1ST DRIVE", True),
            
            # Ordinal number variations (potential failure points)
            ("123 1ST ST", "123 FIRST ST", True),
            ("456 2ND AVE", "456 SECOND AVE", True),
            ("789 3RD DR", "789 THIRD DR", True),
            ("101 21ST ST", "101 21 ST", True),
            ("202 22ND AVE", "202 22 AVE", True),
            
            # Directional variations
            ("123 N MAIN ST", "123 NORTH MAIN ST", True),
            ("456 SW OAK AVE", "456 SOUTHWEST OAK AVE", True),
            ("789 E 1ST ST", "789 EAST FIRST ST", True),
            
            # Spacing and formatting issues
            ("123  MAIN   ST", "123 MAIN ST", True),
            ("456 OAK-AVE", "456 OAK AVE", True),
            ("789 FIRST ST.", "789 FIRST ST", True),
            
            # Partial vs full addresses
            ("123 MAIN ST", "123 MAIN ST ANYTOWN FL 12345", True),
            ("456 OAK AVE APT 2", "456 OAK AVE", True),
            
            # Cases that should NOT match
            ("123 MAIN ST", "124 MAIN ST", False),
            ("123 MAIN ST", "123 OAK ST", False),
            ("123 1ST ST", "123 2ND ST", False),
            
            # Edge cases that commonly fail
            ("123 1ST AVE N", "123 FIRST AVENUE NORTH", True),
            ("456 21ST ST SW", "456 21 STREET SOUTHWEST", True),
            ("789 N 33RD PL", "789 NORTH 33 PLACE", True),
            
            # Complex ordinal cases
            ("101 101ST ST", "101 101 ST", True),
            ("202 111TH AVE", "202 111 AVENUE", True),
            
            # Multiple word street names
            ("123 MARTIN LUTHER KING BLVD", "123 MARTIN LUTHER KING BOULEVARD", True),
            ("456 JOHN F KENNEDY DR", "456 JOHN F KENNEDY DRIVE", True),
            
            # Hyphenated addresses
            ("123 TWENTY-FIRST ST", "123 21ST ST", True),
            ("456 FORTY-SECOND AVE", "456 42ND AVE", True),
        ]
        
        return test_cases
    
    def run_comprehensive_analysis(self) -> Dict:
        """Run complete analysis of address matching system"""
        logger.info("🔍 Starting comprehensive address matching analysis...")
        
        # Generate and analyze test cases
        test_cases = self.generate_test_cases()
        patterns = self.identify_failure_patterns(test_cases)
        
        # Calculate success rate
        total_cases = len(test_cases)
        false_negatives = len(patterns['false_negatives'])
        false_positives = len(patterns['false_positives'])
        success_rate = ((total_cases - false_negatives - false_positives) / total_cases) * 100
        
        analysis_report = {
            'summary': {
                'total_test_cases': total_cases,
                'success_rate': success_rate,
                'false_negatives': false_negatives,
                'false_positives': false_positives,
                'edge_cases': len(patterns['edge_cases'])
            },
            'failure_patterns': patterns,
            'recommendations': self.generate_recommendations(patterns),
            'detailed_analysis': {
                'ordinal_handling': self.analyze_ordinal_handling(patterns),
                'directional_handling': self.analyze_directional_handling(patterns),
                'abbreviation_handling': self.analyze_abbreviation_handling(patterns)
            }
        }
        
        return analysis_report
    
    def analyze_ordinal_handling(self, patterns: Dict) -> Dict:
        """Deep dive into ordinal number handling issues"""
        ordinal_analysis = {
            'common_failures': [],
            'edge_cases': [],
            'recommendations': []
        }
        
        for case in patterns['false_negatives']:
            if any('ordinal' in str(flag).lower() for flag in case['analysis']['csv_components'].get('analysis_flags', [])):
                ordinal_analysis['common_failures'].append({
                    'csv': case['csv'],
                    'zaba': case['zaba'],
                    'csv_ordinals': case['analysis']['csv_components']['ordinals'],
                    'zaba_ordinals': case['analysis']['zaba_components']['ordinals']
                })
        
        # Generate specific recommendations for ordinal handling
        if ordinal_analysis['common_failures']:
            ordinal_analysis['recommendations'].extend([
                "Implement fuzzy matching for ordinal numbers",
                "Add support for spelled-out ordinals (FIRST, SECOND, etc.)",
                "Handle edge cases like 11TH, 12TH, 13TH properly",
                "Consider ordinal variations in scoring algorithm"
            ])
            
        return ordinal_analysis
    
    def analyze_directional_handling(self, patterns: Dict) -> Dict:
        """Analyze directional indicator handling"""
        return {
            'abbreviation_variations': ['N/NORTH', 'S/SOUTH', 'E/EAST', 'W/WEST', 'NE/NORTHEAST'],
            'position_variations': ['prefix', 'suffix'],
            'recommendations': [
                "Standardize directional abbreviations",
                "Handle both prefix and suffix directionals",
                "Consider directional order in matching"
            ]
        }
    
    def analyze_abbreviation_handling(self, patterns: Dict) -> Dict:
        """Analyze street type abbreviation handling"""
        return {
            'common_abbreviations': ['ST/STREET', 'AVE/AVENUE', 'DR/DRIVE', 'BLVD/BOULEVARD'],
            'missing_mappings': [],
            'recommendations': [
                "Expand abbreviation dictionary",
                "Handle period variations (ST. vs ST)",
                "Consider case insensitive matching"
            ]
        }
    
    def generate_recommendations(self, patterns: Dict) -> List[str]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        if patterns['false_negatives']:
            recommendations.append(
                f"🔧 Fix {len(patterns['false_negatives'])} false negative cases - addresses that should match but don't"
            )
            
        if patterns['false_positives']:
            recommendations.append(
                f"⚠️ Address {len(patterns['false_positives'])} false positive cases - addresses that shouldn't match but do"
            )
            
        if patterns['edge_cases']:
            recommendations.append(
                f"🎯 Review {len(patterns['edge_cases'])} edge cases with unexpected similarity/match combinations"
            )
            
        # Specific technical recommendations
        recommendations.extend([
            "📈 Implement similarity scoring threshold (currently binary 2+ parts match)",
            "🔤 Add fuzzy string matching for ordinal numbers and spelled-out numbers",
            "📍 Consider implementing weighted component matching (street number = higher weight)",
            "🧮 Add address parsing library for better component extraction",
            "📊 Implement confidence scoring for matches instead of binary pass/fail",
            "🔄 Add fallback matching strategies for edge cases",
            "📝 Log detailed match attempts for continuous improvement"
        ])
        
        return recommendations
    
    def export_analysis(self, analysis: Dict, filename: str = "address_matching_analysis.json"):
        """Export analysis results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        logger.info(f"📊 Analysis exported to {filename}")

def main():
    """Run comprehensive address matching analysis"""
    analyzer = AddressMatchingAnalyzer()
    
    print("🔍 ZabaSearch Address Matching System Analysis")
    print("=" * 60)
    
    # Run comprehensive analysis
    analysis = analyzer.run_comprehensive_analysis()
    
    # Display summary
    summary = analysis['summary']
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"   Total test cases: {summary['total_test_cases']}")
    print(f"   Success rate: {summary['success_rate']:.1f}%")
    print(f"   False negatives: {summary['false_negatives']}")
    print(f"   False positives: {summary['false_positives']}")
    print(f"   Edge cases: {summary['edge_cases']}")
    
    # Display key failure patterns
    if analysis['failure_patterns']['false_negatives']:
        print(f"\n❌ TOP FALSE NEGATIVES (Should match but don't):")
        for i, case in enumerate(analysis['failure_patterns']['false_negatives'][:3]):
            print(f"   {i+1}. CSV: '{case['csv']}' vs Zaba: '{case['zaba']}'")
            print(f"      Reasons: {', '.join(case['analysis']['failure_reasons'])}")
    
    # Display recommendations
    print(f"\n💡 KEY RECOMMENDATIONS:")
    for i, rec in enumerate(analysis['recommendations'][:5], 1):
        print(f"   {i}. {rec}")
    
    # Export detailed analysis
    analyzer.export_analysis(analysis)
    
    print(f"\n✅ Complete analysis exported to address_matching_analysis.json")

if __name__ == "__main__":
    main()
