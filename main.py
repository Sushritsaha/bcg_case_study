"""
Main entry point for US Vehicle Accident Analysis
This module initializes the Spark session and runs the analysis pipeline.
"""

import logging
import argparse
import os
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession
from src.utils import ConfigUtils
from src.crash_analysis_processor import CrashAnalysisProcessor

def setup_logging() -> None:
    """
    Configure logging for the application
    Sets up both file and console handlers with appropriate formatting
    """
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/vehicle_analysis_{timestamp}.log'
    
    # Set up logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Set up file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized. Log file: {log_filename}")

def create_spark_session(app_name: str = "USVehicleAccidentAnalysis") -> SparkSession:
    """
    Create and configure Spark session
    Args:
        app_name: Name of the Spark application
    Returns:
        Configured SparkSession instance
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration dictionary
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        KeyError: If required configuration is missing
        ValueError: If configuration values are invalid
    """
    required_keys = ["INPUT_FILENAME", "OUTPUT_PATH", "FILE_FORMAT"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required configuration key: {key}")
    
    required_input_files = [
        "Charges", "Damages", "Endorse", "Primary_Person", 
        "Units", "Restrict"
    ]
    for file in required_input_files:
        if file not in config["INPUT_FILENAME"]:
            raise KeyError(f"Missing required input file configuration: {file}")
    
    required_outputs = [str(i) for i in range(1, 11)]
    for output in required_outputs:
        if output not in config["OUTPUT_PATH"]:
            raise KeyError(f"Missing required output path configuration: {output}")

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load and validate configuration from YAML file
    Args:
        config_path: Path to config file
    Returns:
        Configuration dictionary
    Raises:
        FileNotFoundError: If config file doesn't exist
        KeyError: If required configuration is missing
    """
    logging.info(f"Loading configuration from: {config_path}")
    config = ConfigUtils.read_yaml(config_path)
    validate_config(config)
    logging.info("Configuration validated successfully")
    return config

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='US Vehicle Accident Analysis using PySpark'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    return parser.parse_args()

def run_analysis(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Run the vehicle accident analysis pipeline
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
    """
    try:
        output_paths = config["OUTPUT_PATH"]
        file_format = config.get("FILE_FORMAT", {}).get("Output", "csv")
        
        logging.info("Initializing analysis with configuration...")
        analyzer = CrashAnalysisProcessor(spark, config)
        
        # Run analysis queries
        logging.info("1. Analyzing male fatality crashes...")
        result = analyzer.analyze_male_fatality_crashes(output_paths["1"], file_format)
        logging.info(f"Result 1: {result} crashes found with male fatalities")
        
        logging.info("2. Analyzing two-wheeler crashes...")
        result = analyzer.analyze_two_wheeler_crashes(output_paths["2"], file_format)
        logging.info(f"Result 2: {result} two-wheeler crashes found")
        
        logging.info("3. Analyzing fatal crashes without airbags...")
        result = analyzer.analyze_fatal_crashes_without_airbags(output_paths["3"], file_format)
        logging.info(f"Result 3: Top 5 vehicle makes: {result}")
        
        logging.info("4. Analyzing hit and run crashes...")
        result = analyzer.analyze_hit_and_run_crashes(output_paths["4"], file_format)
        logging.info(f"Result 4: {result} hit and run crashes with valid licenses")
        
        logging.info("5. Analyzing states with female crashes...")
        result = analyzer.analyze_state_with_female_crashes(output_paths["5"], file_format)
        logging.info(f"Result 5: State with highest female crashes: {result}")
        
        logging.info("6. Analyzing vehicle makes by injuries...")
        result = analyzer.analyze_vehicle_makes_by_injuries(output_paths["6"], file_format)
        logging.info(f"Result 6: Top 3rd to 5th vehicle makes: {result}")
        
        logging.info("7. Analyzing ethnic crash distribution...")
        result = analyzer.analyze_ethnic_crash_distribution(output_paths["7"], file_format)
        logging.info(f"Result 7: Ethnic distribution by body style written to output file: {result}")
        
        logging.info("8. Analyzing alcohol-related crashes by zip...")
        result = analyzer.analyze_alcohol_related_crashes_by_top5_zip(output_paths["8"], file_format)
        logging.info(f"Result 8: Top 5 zip codes: {result}")
        
        logging.info("9. Analyzing crashes with damage and insurance...")
        result = analyzer.analyze_crashes_with_damage4_avails_insurance(output_paths["9"], file_format)
        logging.info(f"Result 9: {len(result)} crash IDs found: {result}")
        
        logging.info("10. Analyzing top vehicle makes by speeding offenses...")
        result = analyzer.analyze_top5_vehicle_speeding_offenses(output_paths["10"], file_format)
        logging.info(f"Result 10: Top 5 vehicle makes: {result}")
        
        logging.info("All analyses completed successfully")
        
    except Exception as e:
        logging.error(f"Error in analysis pipeline: {str(e)}")
        raise

def main() -> None:
    """Main entry point of the application"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Setup logging
        setup_logging()
        logging.info("Starting US Vehicle Accident Analysis")
        
        # Create Spark session
        logging.info("Initializing Spark session...")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        
        # Load configuration
        config = load_config(args.config)
        
        # Run analysis
        run_analysis(spark, config)
        
        logging.info("Analysis completed successfully")
        
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {str(e)}")
        raise
    except KeyError as e:
        logging.error(f"Invalid configuration: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            logging.info("Stopping Spark session...")
            spark.stop()
            logging.info("Spark session stopped")

if __name__ == "__main__":
    main()
