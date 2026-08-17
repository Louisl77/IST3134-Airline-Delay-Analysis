# IST3134-Airline-Delay-Analysis
Big Data Analytics of US Domestic Flight Delays and Cancellations (2009-2018) using Apache Spark and AWS EMR
# IST3134 Airline Delay Analysis

## Project Overview

This project analyses US domestic airline delays and cancellations from 2009 to 2018 using Big Data technologies.

The complete dataset contains more than 60 million flight records. Apache Spark and Amazon EMR were used to process the large dataset, while Amazon S3 was used for data storage.

## Technologies Used

- Python
- PySpark
- Pandas
- Apache Spark
- Amazon EMR
- Amazon S3
- Microsoft Excel

## Data Processing Workflow

Raw CSV Files → Amazon S3 → Amazon EMR → PySpark Processing → Parquet / Analysis Output → Amazon S3

## Analysis

The project investigates:

- Yearly flight performance
- Airline performance
- Airport and route performance
- Major causes of flight delays
- Cancellation reasons
- Monthly and hourly delay patterns

## Implementation

The 2009 dataset was first used to test the preprocessing and analysis workflow. The final PySpark analysis was then performed on the complete 2009–2018 dataset using Amazon EMR.

Pandas was also used on the 2009 dataset to provide a comparison with the PySpark implementation.

## Dataset

Airline Delay and Cancellation Data, 2009–2018  
Source: Kaggle

## Course

IST3134 Big Data Analytics
