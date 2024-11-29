# US Vehicle Accident Analysis

A PySpark-based analysis application for processing and analyzing vehicle accident data.

## Overview

This project analyzes vehicle accident data using PySpark, providing insights through various analytical queries. The application is designed to be modular and config-driven.

## Prerequisites

- Python 3.7+
- Apache Spark 3.5.3
- PyYAML 5.4.1+

### Dependencies
All required Python packages are listed in `requirements.txt`:
- pyspark>=3.0.0
- PyYAML>=5.4.1
- setuptools>=42.0.0

## Installation

1. Set up Spark:
   - Download Spark 3.5.3 from [Apache Spark](https://spark.apache.org/downloads.html)
   - Extract to `~/Downloads/spark-3.5.3-bin-hadoop3`
   - Ensure Spark is executable: `chmod +x ~/Downloads/spark-3.5.3-bin-hadoop3/bin/spark-submit`

2. Install dependencies:
```bash
make setup
```

## Dataset

The analysis uses 6 CSV files in the Data folder:
- `Charges_use.csv`: Contains information about charges filed in accidents
- `Damages_use.csv`: Details about damages to vehicles and property
- `Endorse_use.csv`: Driver endorsement information
- `Primary_Person_use.csv`: Primary person details in each accident
- `Units_use.csv`: Information about vehicles involved
- `Restrict_use.csv`: Driver restriction details

## Analytics

Application performs the following analyses and stores results:

1. Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2
2. Analytics 2: Find the number of two-wheelers booked for crashes
3. Analytics 3: Determine the top 5 Vehicle Makes of the cars present in the crashes where driver died and airbags did not deploy
4. Analytics 4: Determine the number of Vehicles with driver having valid licenses involved in hit and run
5. Analytics 5: Which state has highest number of accidents in which females are not involved?
6. Analytics 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7. Analytics 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
8. Analytics 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9. Analytics 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10. Analytics 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences

## Running the Analysis

1. Prepare the environment:
```bash
make setup
```

2. Run all analyses:
```bash
make all
```

Or run steps individually:
```bash
make setup     # Install dependencies
make prep_data # Prepare data files
make build    # Build package
make run      # Run analysis
```

## Configuration

The application uses:
- `config.yaml`: Contains:
  - Input data paths
  - Output directory paths
  - Analysis-specific parameters
  - Logging configuration
- `Makefile`: Automates:
  - Environment setup
  - Package building
  - Spark submission
  - Data preparation
- Spark settings in environment:
  - Master: local[*] (uses all available cores)
  - Memory: 4G for driver and executor
  - Python egg file for dependencies

## Output

Results are stored in the `Output` directory:
- Each analysis has its own subdirectory (Output/1 to Output/10)
- Results are saved in CSV format
- Logs are stored in the `logs` directory

## Project Structure

```
bcg_case_study/
├── Data/                           # Input data directory
│   ├── Charges_use.csv            # Vehicle/driver charges information
│   ├── Damages_use.csv            # Vehicle damage details
│   ├── Endorse_use.csv           # Driver endorsements
│   ├── Primary_Person_use.csv    # Primary person in accidents
│   ├── Restrict_use.csv          # Driver restrictions
│   └── Units_use.csv             # Vehicle unit information
│
├── Output/                        # Analysis results directory
│   ├── 1/                        # Male fatality analysis
│   ├── 2/                        # Two-wheeler analysis
│   ├── 3/                        # Vehicle make analysis
│   ├── 4/                        # License analysis
│   ├── 5/                        # State-wise analysis
│   ├── 6/                        # VEH_MAKE_ID analysis
│   ├── 7/                        # Body style analysis
│   ├── 8/                        # Zip code analysis
│   ├── 9/                        # Damage analysis
│   └── 10/                       # Vehicle make/speeding analysis
│
├── src/                          # Source code directory
│   ├── __init__.py              # Package initializer
│   ├── utils.py                 # Utility functions
│   └── us_vehicle_accident_analysis.py  # Core analysis logic
│
├── logs/                         # Log files directory
│   └── app.log                  # Application logs
│
├── main.py                      # Application entry point
├── config.yaml                  # Configuration settings
├── requirements.txt             # Python dependencies
├── setup.py                     # Package setup configuration
├── Makefile                     # Build automation
└── README.md                    # Project documentation
```

### Key Components

1. **Data Files**
   - `Charges_use.csv`: Traffic violations and charges
   - `Damages_use.csv`: Vehicle and property damage details
   - `Endorse_use.csv`: Driver license endorsements
   - `Primary_Person_use.csv`: Main person involved in accident
   - `Restrict_use.csv`: Driver license restrictions
   - `Units_use.csv`: Vehicle details and circumstances

2. **Source Code**
   - `main.py`: Entry point, handles argument parsing and execution
   - `src/us_vehicle_accident_analysis.py`: Core analysis implementation
   - `src/utils.py`: Helper functions and utilities

3. **Configuration**
   - `config.yaml`: Data paths and analysis parameters
   - `setup.py`: Package metadata and dependencies
   - `requirements.txt`: Python package requirements

4. **Build & Automation**
   - `Makefile`: Build and run automation
   - Output directories (1-10): Analysis results
   - `logs/`: Application execution logs

## Troubleshooting

1. Spark Submit Permission Error:
```bash
chmod +x ~/Downloads/spark-3.5.3-bin-hadoop3/bin/spark-submit
```

2. Memory Issues:
- Increase driver/executor memory in Makefile:
```makefile
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=8g
```

3. Python Version Mismatch:
- Ensure Python 3.7+ is installed
- Check virtual environment if used

4. Data File Issues:
- Run `make prep_data` to reset data files
- Verify CSV files in Data directory

## Author

- Sushrit Saha (sushrit.saha@outlook.com)