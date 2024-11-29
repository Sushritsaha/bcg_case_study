"""
Utility functions for US Vehicle Accident Analysis
This module provides utility functions for data loading, configuration management,
and output handling.
"""

import yaml
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import logging
from pathlib import Path


class DataUtils:
    """Utility class for data operations"""
    
    @staticmethod
    def load_csv_data_to_df(spark: SparkSession, file_path: str) -> DataFrame:
        """
        Read CSV data into a Spark DataFrame
        
        Args:
            spark: SparkSession instance
            file_path: Path to the CSV file
        Returns:
            DataFrame: Loaded Spark DataFrame
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
            
        try:
            return spark.read.option("inferSchema", "true").csv(file_path, header=True)
        except Exception as e:
            logging.error(f"Error loading CSV file {file_path}: {str(e)}")
            raise


class ConfigUtils:
    """Utility class for configuration management"""
    
    @staticmethod
    def read_yaml(file_path: str) -> Dict[str, Any]:
        """
        Read configuration from YAML file
        Args:
            file_path: Path to config.yaml
        Returns:
            Dict containing configuration details
        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            yaml.YAMLError: If YAML parsing fails
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Config file not found: {file_path}")
            
        try:
            with open(file_path, "r") as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file {file_path}: {str(e)}")
            raise


class OutputUtils:
    """Utility class for output operations"""
    
    @staticmethod
    def write_output(df: DataFrame, file_path: str, write_format: str = "csv") -> None:
        """
        Write DataFrame to output file
        Args:
            df: Spark DataFrame to write
            file_path: Output file path
            write_format: Output file format (default: csv)
        Returns:
            None
        """
        try:
            if df.rdd.isEmpty():
                # Create empty DataFrame with schema
                empty_df = df.limit(0)
                empty_df.repartition(1).write.format(write_format).mode("overwrite").option(
                    "header", "true"
                ).save(file_path)
            else:
                df.repartition(1).write.format(write_format).mode("overwrite").option(
                    "header", "true"
                ).save(file_path)
        except Exception as e:
            logging.error(f"Error writing output to {file_path}: {str(e)}")
            raise
