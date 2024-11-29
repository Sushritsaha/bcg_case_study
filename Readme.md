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
   - Set SPARK_HOME environment variable to your Spark installation directory
   - Ensure Spark is executable: `chmod +x $SPARK_HOME/bin/spark-submit`

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

## Expected Output

1. Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2. Code should be properly organized in folders as a project.
3. Input data sources and output should be config driven
4. Code should be strictly developed using Data Frame APIs (Do not use Spark SQL)
5. Share the entire project as zip or link to project in GitHub repo.

## Considerations
- Tested on MacOS

## Running the Analysis

### Quick Start
Run all analyses with default settings:
```bash
make all
```

### Step by Step
Run individual steps:
```bash
make setup      # Install dependencies
make prep_data  # Prepare data files
make build     # Build package
make run       # Run analysis
```

### Customizing Execution
You can customize the execution by setting environment variables:

```bash
# Example: Running with custom Spark settings
SPARK_HOME=/path/to/spark \
SPARK_MASTER=spark://master:7077 \
SPARK_EXECUTOR_MEMORY=4g \
make run
```

Available environment variables:
- `SPARK_HOME`: Path to Spark installation
- `PYTHON`: Python interpreter to use (default: python3)
- `SPARK_MASTER`: Spark master URL (default: local[*])
- `SPARK_APP_NAME`: Application name
- `SPARK_EXECUTOR_MEMORY`: Executor memory (default: 2g)
- `SPARK_DRIVER_MEMORY`: Driver memory (default: 2g)
- `SPARK_SHUFFLE_PARTITIONS`: Number of shuffle partitions (default: 8)
- `CONFIG_FILE`: Path to config file (default: config.yaml)

View all options and current settings:
```bash
make help      # Display help
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

- `Makefile`: Automates and configures:
  - Environment setup
  - Package building
  - Spark submission
  - Data preparation
  - Memory settings
  - Parallelism configuration

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
│   ├── Endorse_use.csv            # Driver endorsements
│   ├── Primary_Person_use.csv     # Primary person in accidents
│   ├── Restrict_use.csv           # Driver restrictions
│   └── Units_use.csv              # Vehicle unit information
│
├── src/                            # Source code directory
│   ├── __init__.py                 # Package initializer
│   ├── crash_analysis_processor.py # Core analysis logic
│   └── utils.py                    # Utility functions
│
├── Output/                       # Analysis results directory
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
├── logs/                         # Log files directory
├── build/                        # Build artifacts
├── dist/                         # Distribution packages
├── Data.zip                      # Compressed data file
├── main.py                       # Application entry point
├── config.yaml                   # Configuration settings
├── requirements.txt              # Python dependencies
├── setup.py                      # Package setup configuration
├── Makefile                      # Build automation
└── README.md                     # Project documentation
```

## Error Handling

The Makefile includes various safety checks:
- Verifies Spark installation
- Checks for required files
- Validates environment setup
- Provides clear error messages

## Runbook

Clone the repo and follow these steps:

### Considerations
- Tested on MacOS

### Quick Start Steps
1. Go to the Project Directory:
   ```bash
   cd bcg_case_study
   ```

2. View available commands:
   ```bash
   make help
   ```

3. Prepare the environment:
   ```bash
   make setup
   make prep_data
   ```

4. Build the project:
   ```bash
   make build
   ```

5. Run the analysis:
   ```bash
   make run
   ```

### Manual Spark Submit
If you prefer to run spark-submit directly:
```bash
spark-submit \
    --master "local[*]" \
    --py-files dist/bcg_case_study_29nov-0.0.1-py3.10.egg \
    main.py --config config.yaml
```

## Author
- Sushrit Saha (sushrit.saha@outlook.com)