"""
BCG Case Study - US Vehicle Accident Analysis
A PySpark-based analysis of US vehicle accident data
"""

__author__ = "sushrit.saha"

from setuptools import find_packages, setup

setup(
    name="bcg_case_study_29nov",
    version="0.0.1",
    description="US Vehicle Accident Analysis using PySpark",
    author="sushrit.saha",
    author_email="sushrit.saha@outlook.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "PyYAML>=5.4.1",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Data Scientists",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering :: Data Analysis",
    ],
    keywords="pyspark, vehicle-accidents, data-analysis, bcg-case-study",
    zip_safe=False,
)
