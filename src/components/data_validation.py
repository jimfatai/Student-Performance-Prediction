import os
import pandas as pd
from mlProject import logger
from mlProject.entity.config_entity import DataValidationConfig

class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_all_columns(self) -> bool:
        try:
            validation_status = True  # Assume validation passes initially

            # Read the dataset
            data = pd.read_csv(self.config.unzip_data_dir)
            all_cols = list(data.columns)

            # Get the expected schema
            all_schema = self.config.all_schema.keys()

            # Validate column names
            for col in all_cols:
                if col not in all_schema:
                    logger.error(f"Column '{col}' not found in schema.")
                    validation_status = False
                else:
                    # Validate data types
                    expected_dtype = self.config.all_schema[col]
                    actual_dtype = str(data[col].dtype)
                    if expected_dtype != actual_dtype:
                        logger.error(f"Data type mismatch for column '{col}'. Expected: {expected_dtype}, Found: {actual_dtype}")
                        validation_status = False

            # Validate missing values
            if data.isnull().sum().any():
                missing_values = data.isnull().sum()
                logger.error(f"Missing values found in columns:\n{missing_values[missing_values > 0]}")
                validation_status = False
            else:
                logger.info("No missing values found in the dataset.")

            # Validate duplicates
            if data.duplicated().any():
                duplicate_count = data.duplicated().sum()
                logger.error(f"Duplicate rows found: {duplicate_count}")
                validation_status = False
            else:
                logger.info("No duplicate rows found in the dataset.")

            # Write the final validation status to the status file
            with open(self.config.STATUS_FILE, 'w') as f:
                f.write(f"Validation status: {validation_status}")

            return validation_status

        except Exception as e:
            logger.error(f"Error during data validation: {e}") 
            raise e .
