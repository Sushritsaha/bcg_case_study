"""
Vehicle Crash Analysis Processor
This module provides analysis functionality for vehicle crash data using PySpark DataFrame APIs.
"""

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, count, desc, sum
from pyspark.sql import Window
import logging
from src.utils import DataUtils, OutputUtils

class CrashAnalysisProcessor:
    """
    Main class for analyzing vehicle crash data
    
    This class provides methods to analyze various aspects of vehicle crashes
    using PySpark DataFrame operations.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize with SparkSession and config
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        Raises:
            KeyError: If required configuration keys are missing
            FileNotFoundError: If input files are not found
        """
        try:
            input_file_paths = config["INPUT_FILENAME"]
            self.df_charges = DataUtils.load_csv_data_to_df(spark, input_file_paths["Charges"])
            self.df_damages = DataUtils.load_csv_data_to_df(spark, input_file_paths["Damages"])
            self.df_endorse = DataUtils.load_csv_data_to_df(spark, input_file_paths["Endorse"])
            self.df_primary_person = DataUtils.load_csv_data_to_df(spark, input_file_paths["Primary_Person"])
            self.df_units = DataUtils.load_csv_data_to_df(spark, input_file_paths["Units"])
            self.df_restrict = DataUtils.load_csv_data_to_df(spark, input_file_paths["Restrict"])
        except KeyError as e:
            logging.error(f"Missing configuration key: {str(e)}")
            raise
        except FileNotFoundError as e:
            logging.error(f"Input file not found: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error initializing CrashAnalysisProcessor: {str(e)}")
            raise

    def analyze_male_fatality_crashes(self, output_path: str, output_format: str) -> int:
        """
        Analysis 1: Find the number of crashes (accidents) in which in which number of males killed are greater than 2?
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            Number of crashes with male fatalities > 2
        """
        try:
            male_fatal_crashes = (self.df_primary_person
                .filter(
                    (col("PRSN_GNDR_ID") == "MALE") &
                    (col("DEATH_CNT") > 2)
                )
                .select("CRASH_ID", "PRSN_GNDR_ID", "DEATH_CNT")
            )
            
            OutputUtils.write_output(male_fatal_crashes, output_path, output_format)
            return male_fatal_crashes.count()
        except Exception as e:
            logging.error(f"Error in analyze_male_fatality_crashes: {str(e)}")
            raise

    def analyze_two_wheeler_crashes(self, output_path: str, output_format: str) -> int:
        """
        Analysis 2: How many two-wheelers are booked for crashes?
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            Number of crashes involving 2-wheelers
        """
        try:
            motorcycle_crashes = (self.df_units
                .filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
                .select("CRASH_ID", "VEH_BODY_STYL_ID")
            )
            
            OutputUtils.write_output(motorcycle_crashes, output_path, output_format)
            return motorcycle_crashes.count()
        except Exception as e:
            logging.error(f"Error in analyze_two_wheeler_crashes: {str(e)}")
            raise

    def analyze_fatal_crashes_without_airbags(self, output_path: str, output_format: str) -> List[str]:
        """
        Analysis 3: What are the top 5 Vehicle Makes of cars involved in crashes where the driver died and airbags did not deploy?
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            List of top 5 Vehicle Makes for fatal crashes without airbags
        """
        try:
            fatal_crashes_no_airbag = (self.df_units
                .join(self.df_primary_person, ["CRASH_ID"])
                .filter(
                    (col("PRSN_INJRY_SEV_ID") == "KILLED") &
                    (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
                    (col("VEH_MAKE_ID") != "NA")
                )
                .groupBy("VEH_MAKE_ID")
                .agg(count("*").alias("CRASH_COUNT"))
                .orderBy(col("CRASH_COUNT").desc())
                .limit(5)
            )
            
            OutputUtils.write_output(fatal_crashes_no_airbag, output_path, output_format)
            return [row.VEH_MAKE_ID for row in fatal_crashes_no_airbag.collect()]
        except Exception as e:
            logging.error(f"Error in analyze_fatal_crashes_without_airbags: {str(e)}")
            raise


    def analyze_hit_and_run_crashes(self, output_path: str, output_format: str) -> int:
        """
        Analysis 4: How many vehicles involved in hit-and-run crashes had drivers with valid licenses?
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        
        Returns:
            Number of vehicles involved in hit-and-run incidents with valid licenses
        """
        try:
            valid_licenses = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
            
            hit_and_run_with_license = (self.df_units
                .join(self.df_primary_person, ["CRASH_ID"])
                .filter(
                    (col("VEH_HNR_FL") == "Y") &
                    (col("DRVR_LIC_TYPE_ID").isin(valid_licenses))
                )
                .select("CRASH_ID", "VEH_HNR_FL", "DRVR_LIC_TYPE_ID")
            )
            
            OutputUtils.write_output(hit_and_run_with_license, output_path, output_format)
            return hit_and_run_with_license.count()
        except Exception as e:
            logging.error(f"Error in analyze_hit_and_run_crashes: {str(e)}")
            raise

    def analyze_state_with_female_crashes(self, output_path: str, output_format: str) -> str:
        """
        Analysis 5: Which state has the highest number of accidents in which females are involved?
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            State with highest number of female-involved accidents
        """
        try:
            female_crashes_by_state = (self.df_primary_person
                .filter(col("PRSN_GNDR_ID") == "FEMALE")
                .groupBy("DRVR_LIC_STATE_ID")
                .agg(count("*").alias("FEMALE_CRASH_COUNT"))
                .orderBy(col("FEMALE_CRASH_COUNT").desc())
            )
            
            OutputUtils.write_output(female_crashes_by_state, output_path, output_format)
            return female_crashes_by_state.first().DRVR_LIC_STATE_ID
        except Exception as e:
            logging.error(f"Error in analyze_state_with_female_crashes: {str(e)}")
            raise  

    def analyze_vehicle_makes_by_injuries(self, output_path: str, output_format: str) -> List[str]:
        """
        Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            List of vehicle makes (VEH_MAKE_IDs) ranked 3rd to 5th by total casualties
        """
        try:
            vehicle_casualties = (self.df_units
                .filter(col("VEH_MAKE_ID").isNotNull() & (col("VEH_MAKE_ID") != "NA"))
                .withColumn("TOTAL_CASUALTIES", 
                    col("TOT_INJRY_CNT") + col("DEATH_CNT"))
                .groupBy("VEH_MAKE_ID")
                .agg(sum("TOTAL_CASUALTIES").alias("TOTAL_CASUALTIES_COUNT"))
                .orderBy(col("TOTAL_CASUALTIES_COUNT").desc())
            )
            
            top_5_makes = vehicle_casualties.limit(5)
            top_2_makes = vehicle_casualties.limit(2)
            ranked_3_to_5 = top_5_makes.subtract(top_2_makes)
            
            OutputUtils.write_output(ranked_3_to_5, output_path, output_format)
            return [row.VEH_MAKE_ID for row in ranked_3_to_5.collect()]
        except Exception as e:
            logging.error(f"Error in analyze_vehicle_makes_by_injuries: {str(e)}")
            raise

    def analyze_ethnic_crash_distribution(self, output_path: str, output_format: str) -> DataFrame:
        """
        Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            DataFrame with top ethnic user group for each body style
        """
        try:
            invalid_body_styles = ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
            invalid_ethnicities = ["NA", "UNKNOWN"]
            
            body_style_window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("crash_count").desc())
            
            ethnic_distribution = (self.df_units
                .join(self.df_primary_person, ["CRASH_ID"])
                .filter(~col("VEH_BODY_STYL_ID").isin(invalid_body_styles))
                .filter(~col("PRSN_ETHNICITY_ID").isin(invalid_ethnicities))
                .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
                .agg(count("*").alias("crash_count"))
                .withColumn("rank", row_number().over(body_style_window))
                .filter(col("rank") == 1)
                .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            )

            OutputUtils.write_output(ethnic_distribution, output_path, output_format)
            return ethnic_distribution
        except Exception as e:
            logging.error(f"Error in analyze_ethnic_crash_distribution: {str(e)}")
            raise

    def analyze_alcohol_related_crashes_by_top5_zip(self, output_path: str, output_format: str) -> List[str]:
        """
        Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the 
        contributing factor to a crash (Use Driver Zip Code)
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            List of top 5 ZIP codes with highest alcohol-related crashes
        """
        try:
            # Find crashes with alcohol as contributing factor
            alcohol_crashes = (self.df_units
            .join(self.df_primary_person, ["CRASH_ID"])
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") |
                col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupBy("DRVR_ZIP")
            .agg(count("*").alias("crash_count"))
            .orderBy(col("crash_count").desc())
            .limit(5)
        )

            OutputUtils.write_output(alcohol_crashes, output_path, output_format)
            return [row.DRVR_ZIP for row in alcohol_crashes.collect()]
        except Exception as e:
            logging.error(f"Error in analyze_alcohol_related_crashes_by_top5_zip: {str(e)}")
            raise

    def analyze_crashes_with_damage4_avails_insurance(self, output_path: str, output_format: str) -> List[str]:
        """
        Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is 
        above 4 and car avails Insurance

        conditions:
        1. No damaged property was observed
        2. Vehicle damage level is above 4
        3. Vehicle has insurance
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            List of distinct Crash IDs meeting the damage and insurance criteria
        """
        try:
            # Define invalid or excluded damage values
            invalid_damage_values = ["NA", "NO DAMAGE", "INVALID VALUE"]
            
            # Create damage level conditions for both scales
            damage_scale1_condition = (
                (col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4") &
                (~col("VEH_DMAG_SCL_1_ID").isin(invalid_damage_values))
            )
            damage_scale2_condition = (
                (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4") &
                (~col("VEH_DMAG_SCL_2_ID").isin(invalid_damage_values))
            )

            # Find crashes meeting all criteria
            crashes_with_damage = (self.df_damages
                .join(self.df_units, ["CRASH_ID"])
                .filter(
                    # Either damage scale should meet the criteria
                    damage_scale1_condition | damage_scale2_condition
                )
                # Additional criteria
                .filter(col("DAMAGED_PROPERTY") == "NONE")
                .filter(col("FIN_RESP_TYPE_ID") == "PROOF OF LIABILITY INSURANCE")
                # Select only unique crash IDs
                .select("CRASH_ID")
                .distinct()
            )

            OutputUtils.write_output(crashes_with_damage, output_path, output_format)
            return [row.CRASH_ID for row in crashes_with_damage.collect()]
            
        except Exception as e:
            logging.error(f"Error in analyze_crashes_with_damage_insurance: {str(e)}")
            raise

    def analyze_top5_vehicle_speeding_offenses(self, output_path: str, output_format: str) -> List[str]:
        """
        Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed 
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
        
        Args:
            output_path: Path to save the output
            output_format: Format for output file
        Returns:
            List of top 5 Vehicle Makes meeting the specified criteria
        """
        try:
            # Get vehicles from states with highest offences
            states_with_offences = (self.df_units
                .filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
                .groupBy("VEH_LIC_STATE_ID")
                .agg(count("*").alias("offense_count"))
                .orderBy(col("offense_count").desc())
                .limit(25)
            )
            top_25_states = [row.VEH_LIC_STATE_ID for row in states_with_offences.collect()]

            # Find most common vehicle colors
            popular_colors = (self.df_units
                .filter(col("VEH_COLOR_ID") != "NA")
                .groupBy("VEH_COLOR_ID")
                .agg(count("*").alias("color_count"))
                .orderBy(col("color_count").desc())
                .limit(10)
            )
            top_10_colors = [row.VEH_COLOR_ID for row in popular_colors.collect()]

            # Combine all criteria for final analysis
            speeding_vehicles = (self.df_charges
                .join(self.df_primary_person, ["CRASH_ID"])
                .join(self.df_units, ["CRASH_ID"])
                .filter(
                    (col("CHARGE").contains("SPEED")) &
                    (col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) &
                    (col("VEH_COLOR_ID").isin(top_10_colors)) &
                    (col("VEH_LIC_STATE_ID").isin(top_25_states))
                )
                .groupBy("VEH_MAKE_ID")
                .agg(count("*").alias("violation_count"))
                .orderBy(col("violation_count").desc())
                .limit(5)
            )

            OutputUtils.write_output(speeding_vehicles, output_path, output_format)
            return [row.VEH_MAKE_ID for row in speeding_vehicles.collect()]
        except Exception as e:
            logging.error(f"Error in analyze_top5_vehicle_speeding_offenses: {str(e)}")
            raise