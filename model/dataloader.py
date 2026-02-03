import logging
from pathlib import Path
from typing import Optional, List
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd
import joblib

logger=logging.getLogger(__name__)  # logger associated with the current module/file

class DataLoader:
    """Class for data loading and preprocessing"""
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.scaler = StandardScaler()
        self.feature_columns: Optional[List[str]] = None  # Stores the columns used for fitting
        self.label_columns = ['Is_Anomaly', 'Anomaly_Type'] # label columns to exclude in preprocessing (present only in test set)

    def load_data(self, filename: str = "*.csv") -> pd.DataFrame:
        """
        Loads data from the directory
        Args:
            filename: Pattern for files to load
        Returns:
            pd.DataFrame: DataFrame with all loaded data
        """
        data_files=list(self.data_dir.glob(filename))

        if not data_files:
            raise FileNotFoundError (f"No file found with pattern {filename} in {self.data_dir}")
        
        logger.info(f"Loading {len(data_files)} files...")

        dfs= []
        for file_path in data_files:
            try:
                if file_path.suffix == ".csv":
                    df = pd.read_csv(file_path, sep=None, engine="python", encoding_errors="replace")  # auto-detect separator,  more tolerant parser, avoids encoding crashes
                elif file_path.suffix == ".parquet":
                    df= pd.read_parquet(file_path)    
                else:
                    logger.warning(f"Unsupported file type: {file_path}")
                    continue
                logger.info(f" Loaded {file_path.name}: {len(df)} rows, {len(df.columns)} columns")
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
                continue

        if not dfs:
            raise ValueError("No data was loaded successfully")
        
        # Concatenate all DataFrames
        data=pd.concat(dfs, ignore_index= True)
        logger.info(f"Total data loaded: {len(data)} rows, {len(data.columns)} columns")

        return data
    
    def preprocess_data(self, data: pd.DataFrame, fit: bool = True) -> pd.DataFrame:
        """
        Preprocesses data for training
        Args:
            data: DataFrame with data
        Returns:
            features (X)
        """
        logger.info("Starting data preprocessing...")
        
        df=data.copy()

        # 1. Identify all numeric columns
        all_numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        # 2. EXPLICITLY EXCLUDE label columns from columns to scale
        cols_to_scale = [c for c in all_numeric_cols if c not in self.label_columns]

        if not cols_to_scale:
            raise ValueError("No valid numeric columns to scale found")

        if fit:
            # Save exactly which columns we scaled
            self.feature_columns = cols_to_scale
            X_scaled = self.scaler.fit_transform(df[self.feature_columns])
            logger.info(f"Normalization completed on {len(self.feature_columns)} columns")
        else:
            # During testing, use ONLY the columns saved during fit
            X_scaled = self.scaler.transform(df[self.feature_columns])
            logger.info("Normalization (transform) completed")

        # Return as DataFrame with column names
        X = pd.DataFrame(X_scaled, 
                         columns=self.feature_columns, 
                         index=df.index)

        return X
    
    def inverse_transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Performs inverse transform on normalized data
        Args:
            data: DataFrame with normalized data
        Returns:
            DataFrame with data in the original scale
        """
        if self.feature_columns is None:
            raise ValueError("Scaler not yet fitted. Run preprocess_data with fit=True first")
        
        # Check that columns match
        if list(data.columns) != self.feature_columns:
            raise ValueError(
                f"DataFrame columns do not match scaler columns. "
                f"Expected: {self.feature_columns}, Received: {list(data.columns)}"
            )
        
        # Inverse transform
        X_original = self.scaler.inverse_transform(data)
        
        # Return as DataFrame
        result = pd.DataFrame(X_original, columns=self.feature_columns, index=data.index)
        logger.info("Inverse transform completed")
        
        return result


    def save_scaler(self, filepath: Path):
        """
        Saves the scaler for future use (e.g., on test dataset)
        without refitting and without recomputing feature columns
        """
        try:
            # Save both scaler and feature_columns
            scaler_data = {
                'scaler': self.scaler,
                'feature_columns': self.feature_columns
            }
            joblib.dump(scaler_data, filepath)
            logger.info(f"Scaler and feature columns saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving scaler: {e}")
            raise  # re-raise original error, program stops and logs trace

    def load_scaler(self, filepath: Path):
        """Loads a saved scaler"""
        try:
            scaler_data = joblib.load(filepath)
            self.scaler = scaler_data['scaler']
            self.feature_columns = scaler_data['feature_columns']
            logger.info(f"Scaler loaded from {filepath}")
            logger.info(f"Feature columns: {self.feature_columns}")
        except Exception as e:
            logger.error(f"Error loading scaler: {e}")
            raise  # re-raise original error, program stops and logs trace