import pandas as pd
from itertools import combinations
from sqlalchemy import text
from typing import List, Dict, Any

class SchemaRuleMiner:
    """
    SchemaIQ's Proprietary Schema-Aware Association Rule Miner.
    Automatically discovers hidden business logic from categorical and boolean columns.
    """
    def __init__(self, min_support=0.05, min_confidence=0.8):
        self.min_support = min_support
        self.min_confidence = min_confidence
        
    def mine_rules(self, df):
        # 1. Identify categorical columns (low cardinality)
        categorical_cols = [c for c in df.columns if df[c].nunique() > 1 and df[c].nunique() <= 15]
        if not categorical_cols:
            return []
            
        df = df[categorical_cols].copy()
        n_rows = len(df)
        
        # 2. Convert to binary transaction lists
        transactions = []
        for i, row in df.iterrows():
            txn = frozenset([f"{col}={row[col]}" for col in categorical_cols if pd.notna(row[col])])
            transactions.append(txn)
            
        # 3. Find 1-itemsets
        item_counts = {}
        for txn in transactions:
            for item in txn:
                item_counts[item] = item_counts.get(item, 0) + 1
                
        freq_1_items = {item: count for item, count in item_counts.items() if count / n_rows >= self.min_support}
        
        if len(freq_1_items) < 2:
            return []
            
        # 4. Find 2-itemsets
        pair_counts = {}
        for txn in transactions:
            txn_filtered = [item for item in txn if item in freq_1_items]
            for pair in combinations(txn_filtered, 2):
                pair = frozenset(pair)
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
                
        freq_2_items = {pair: count for pair, count in pair_counts.items() if count / n_rows >= self.min_support}
        
        # 5. Generate Rules (A => B)
        rules: List[Dict[str, Any]] = []
        for pair, count in freq_2_items.items():
            item_a, item_b = tuple(pair)
            
            # Rule: A => B
            # Calculate confidence
            conf_a_b = count / freq_1_items[item_a]
            if conf_a_b >= self.min_confidence:
                # To prevent trivial rules, make sure B is not just naturally 99% of the dataset
                lift = conf_a_b / (freq_1_items[item_b] / n_rows)
                if lift > 1.2:
                    rules.append({
                        "antecedent": item_a,
                        "consequent": item_b,
                        "confidence": round(conf_a_b * 100, 1),
                        "lift": round(lift, 2),
                        "support": round(count / n_rows * 100, 1)
                    })
                    
            # Rule: B => A
            conf_b_a = count / freq_1_items[item_b]
            if conf_b_a >= self.min_confidence:
                lift = conf_b_a / (freq_1_items[item_a] / n_rows)
                if lift > 1.2:
                    rules.append({
                        "antecedent": item_b,
                        "consequent": item_a,
                        "confidence": round(conf_b_a * 100, 1),
                        "lift": round(lift, 2),
                        "support": round(count / n_rows * 100, 1)
                    })
                    
        # Sort by confidence + lift
        rules.sort(key=lambda x: (float(x.get("confidence", 0)), float(x.get("lift", 0))), reverse=True)
        # Replaced rules[:5] with an explicit loop to avoid Pyright/Pyre slice resolution bug
        return [rules[i] for i in range(min(5, len(rules)))]

def discover_table_rules(engine, tname, limit=5000):
    try:
        with engine.connect() as conn:
            query = f'SELECT * FROM "{tname}" LIMIT {limit}'
            df = pd.read_sql(text(query), conn)
            
            if len(df) < 50:
                return []
                
            miner = SchemaRuleMiner(min_support=0.03, min_confidence=0.85)
            rules = miner.mine_rules(df)
            
            formatted = []
            for r in rules:
                ant_col, ant_val = r["antecedent"].split("=", 1)
                con_col, con_val = r["consequent"].split("=", 1)
                formatted.append({
                    "rule_text": f"If {ant_col} is '{ant_val}', then {con_col} is almost always '{con_val}'.",
                    "confidence": f"{r['confidence']}%",
                    "lift": r["lift"]
                })
            return formatted
            
    except Exception as e:
        print(f"Rule discovery failed for {tname}: {e}")
        return []
