import numpy as np

class CustomIsolationTree:
    """
    A single tree in the Custom Variance-Weighted Isolation Forest.
    """
    def __init__(self, height_limit):
        self.height_limit = height_limit
        self.split_attribute = None
        self.split_value = None
        self.left_branch = None
        self.right_branch = None
        self.size = 0
        self.is_leaf = False

    def fit(self, X, current_height=0):
        self.size = X.shape[0]
        
        # Stopping criteria
        if current_height >= self.height_limit or self.size <= 1:
            self.is_leaf = True
            return self

        # HACKATHON NOVELTY: Variance-Weighted Feature Selection
        # Instead of np.random.choice(features), we pick features based on their variance in the current subspace.
        # This forces the tree to split on metrics that actually vary (like massive freight_values or prices)
        # instead of uniformly spreading splits across low-variance categorical/encoded features.
        variances = np.var(X, axis=0)
        sum_var = np.sum(variances)
        
        if sum_var == 0:
            # All data points in this node are identical; we cannot split further.
            self.is_leaf = True
            return self
            
        probs = variances / sum_var
        num_features = X.shape[1]
        
        # Select an attribute proportionally to its variance
        self.split_attribute = np.random.choice(num_features, p=probs)
        
        # Standard step: pick a random split value between min and max
        min_val = np.min(X[:, self.split_attribute])
        max_val = np.max(X[:, self.split_attribute])
        
        if min_val == max_val:
            self.is_leaf = True
            return self

        self.split_value = np.random.uniform(min_val, max_val)

        # Partition data
        left_mask = X[:, self.split_attribute] < self.split_value
        right_mask = ~left_mask

        # Recursively build branches
        self.left_branch = CustomIsolationTree(self.height_limit)
        self.left_branch.fit(X[left_mask], current_height + 1)

        self.right_branch = CustomIsolationTree(self.height_limit)
        self.right_branch.fit(X[right_mask], current_height + 1)

        return self

    def path_length(self, x, current_height):
        if self.is_leaf:
            # Add the BST adjustment factor (c(n)) to account for the unbuilt branches above size 1
            return current_height + self._calculate_c(self.size)

        if x[self.split_attribute] < self.split_value:
            return self.left_branch.path_length(x, current_height + 1)
        else:
            return self.right_branch.path_length(x, current_height + 1)

    def _calculate_c(self, n):
        """Average path length of unsuccessful search in BST."""
        if n <= 1:
            return 0
        elif n == 2:
            return 1
        return 2.0 * (np.log(n - 1) + 0.5772156649) - (2.0 * (n - 1.) / n)

class SchemaIQIsolationForest:
    """
    SchemaIQ's Proprietary Variance-Weighted Isolation Forest (VW-iForest).
    Rewritten entirely from scratch using NumPy.
    """
    def __init__(self, n_estimators=100, max_samples='auto', contamination=0.05):
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.trees = []
        self.c_value = 0

    def fit(self, X):
        # Convert to numpy array safely
        if not isinstance(X, np.ndarray):
            X = np.array(X)
            
        n_samples = X.shape[0]
        
        if self.max_samples == 'auto':
            self.max_samples_val = min(256, n_samples)
        else:
            self.max_samples_val = self.max_samples

        self.height_limit = np.ceil(np.log2(self.max_samples_val)).astype(int)
        self.c_value = self._calculate_c(self.max_samples_val)
        
        self.trees = []
        for _ in range(self.n_estimators):
            # Subsample the data without replacement
            indices = np.random.choice(n_samples, self.max_samples_val, replace=False)
            X_sub = X[indices]
            
            tree = CustomIsolationTree(self.height_limit)
            tree.fit(X_sub)
            self.trees.append(tree)

    def _calculate_c(self, n):
        if n <= 1: return 0
        elif n == 2: return 1
        return 2.0 * (np.log(n - 1) + 0.5772156649) - (2.0 * (n - 1.) / n)

    def anomaly_scores(self, X):
        """
        Returns an anomaly score between 0 and 1 for each observation.
        Values closer to 1 indicate anomalies.
        """
        if not isinstance(X, np.ndarray):
            X = np.array(X)
            
        n_samples = X.shape[0]
        paths = np.zeros((n_samples, self.n_estimators))

        for i in range(n_samples):
            for j, tree in enumerate(self.trees):
                paths[i, j] = tree.path_length(X[i], 0)

        # Average path length across all trees
        avg_paths = np.mean(paths, axis=1)
        
        # Calculate final anomaly score
        # Using the standard IF formula: 2 ^ -( E(h(x)) / c(n) )
        scores = 2.0 ** (-avg_paths / self.c_value)
        return scores

    def predict(self, X):
        scores = self.anomaly_scores(X)
        threshold = np.percentile(scores, 100 * (1 - self.contamination))
        return (scores >= threshold).astype(int), scores

# ── Dynamic API Interface ──────────────────────────────────────────────────
import pandas as pd
from sqlalchemy import text

def detect_table_anomalies(engine, tname, limit=5000):
    """
    Fetch numerical columns from a table and run the VW-iForest to find anomalous rows.
    Returns the top anomalies as a list of dictionaries with their scores.
    """
    try:
        with engine.connect() as conn:
            # Only select numerical columns
            from sqlalchemy import inspect
            insp = inspect(engine)
            cols = insp.get_columns(tname)
            num_cols = [c["name"] for c in cols if 'int' in str(c["type"]).lower() or 'float' in str(c["type"]).lower() or 'numeric' in str(c["type"]).lower() or 'real' in str(c["type"]).lower()]
            
            if len(num_cols) == 0:
                return [] # No numerical columns to find anomalies on
                
            query = f'SELECT * FROM "{tname}" LIMIT {limit}'
            df = pd.read_sql(text(query), conn)
            
            if len(df) < 20: 
                return [] # Not enough data
                
            # Fill NaNs for the model
            X = df[num_cols].fillna(0).values
            
            model = SchemaIQIsolationForest(n_estimators=50, contamination=0.01)
            model.fit(X)
            predictions, scores = model.predict(X)
            
            # Extract top 5 anomalies
            anomaly_indices = np.where(predictions == 1)[0]
            top_anomalies = []
            
            for idx in anomaly_indices:
                record = df.iloc[idx].to_dict()
                top_anomalies.append({
                    "score": round(float(scores[idx]), 3),
                    "record": record
                })
            
            # Sort by score descending and keep top 5
            top_anomalies.sort(key=lambda x: x["score"], reverse=True)
            return top_anomalies[:5]
            
    except Exception as e:
        print(f"Anomaly detection failed for {tname}: {e}")
        return []

