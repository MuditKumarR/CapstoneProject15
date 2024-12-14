# Sports Data Processing Pipeline

This repository contains a Luigi-based data pipeline for processing sports data stored in a CSV file on AWS S3 and loading it into a MySQL RDS database. The pipeline involves several stages, including data download, exploration, cleaning, and loading to the database, followed by a query for analysis. The pipeline is designed to handle missing values, anomalies, and outliers effectively.

## Table of Contents
1. [Overview](#overview)
2. [Libraries Used](#libraries-used)
3. [Code Explanation](#code-explanation)
4. [Setup and Configuration](#setup-and-configuration)
    1. [Getting AWS Access Keys](#getting-aws-access-keys)
    2. [Creating a MySQL RDS Instance](#creating-a-mysql-rds-instance)
5. [How to Run](#how-to-run)
6. [License](#license)

## Overview
This project consists of a set of Luigi tasks that automate the workflow of:
1. **Downloading Data** from an S3 bucket.
2. **Exploring the Data** to generate summary statistics and visualizations.
3. **Cleaning the Data** by handling missing values, correcting anomalies, and removing outliers.
4. **Loading the Cleaned Data** into a MySQL RDS database.
5. **Querying the Data** from MySQL RDS to perform analysis.

Each task is designed to be modular and can be executed independently or as part of the complete pipeline.

## Libraries Used

The following libraries and tools are used in this pipeline:

1. **pandas**: Used for data manipulation, including loading CSV files, handling missing data, and cleaning data.
2. **numpy**: Used for numerical operations, including handling missing values and outlier detection.
3. **scikit-learn**: Specifically the `SimpleImputer` class, used for imputing missing data (mean for numeric columns and most frequent for categorical columns).
4. **scipy**: Used for calculating Z-scores to identify anomalies in numerical data.
5. **boto3**: AWS SDK for Python, used for interacting with AWS services like S3 to download files.
6. **sqlalchemy**: Used to interact with MySQL databases, enabling reading from and writing to MySQL using the `to_sql` function.
7. **mysql-connector-python**: Used to query the MySQL RDS database.
8. **luigi**: A Python module for building complex pipelines of batch jobs. It defines tasks, their dependencies, and executes them in order.
9. **tqdm**: A library for displaying progress bars in loops.
10. **matplotlib** and **seaborn**: Used for data visualization, such as plotting distributions of missing values.

## Code Explanation

### Task 1: `DownloadData`
This task downloads a CSV file from an AWS S3 bucket and saves it locally.

- **AWS Configuration**: The `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` are used to authenticate with AWS.
- **S3 Download**: The CSV file is fetched from the S3 bucket `capstone-group15`, specifically from the file `Sports_Dataset_1M.csv`.
- **Progress Bar**: The file download progress is shown using `tqdm` to provide real-time feedback during the download process.

### Task 2: `ExploreData`
This task reads the downloaded CSV file and generates an exploration report with basic statistics.

- **Missing Values**: It identifies columns with missing values.
- **Data Types**: It checks for columns that should be converted to datetime or integer types.
- **Visualizations**: Histograms are generated for columns with missing values to help visualize their distributions.

### Task 3: `CleanData`
This task processes the dataset to handle missing values and anomalies:

- **Missing Values**: 
  - For numeric columns (e.g., `Attendance`, `Goals`, etc.), the missing values are imputed with the mean.
  - For categorical columns (e.g., `Weather`), the missing values are imputed with the most frequent category.
- **Anomalies**: 
  - Z-scores are calculated for numeric columns, and rows with values that have a Z-score greater than 3 are considered anomalies and removed.
  - Outliers are identified using the IQR method and removed from the dataset.
- **Data Cleaning**: The cleaned dataset is saved to a new CSV file.

### Task 4: `LoadToRDS`
This task loads the cleaned data into a MySQL RDS database.

- **SQLAlchemy**: The connection to the MySQL database is established using the SQLAlchemy engine. The cleaned DataFrame is then written to the `sports_data` table.
- **Table Overwrite**: The data is written with the `if_exists='replace'` parameter, which overwrites the existing table in the database.

### Task 5: `QueryRDS`
This task queries the MySQL database for insights, such as the average attendance grouped by weather conditions.

- **SQL Query**: The query selects the `Weather` column and the average of the `Attendance` column from the `sports_data` table, grouped by `Weather`.
- **Results**: The results of the query are written to an output text file.

## Setup and Configuration

Before running the pipeline, you need to configure the necessary credentials and dependencies.

### Required Libraries
Install the required libraries by running:

```bash
pip install pandas numpy scikit-learn boto3 sqlalchemy mysql-connector-python luigi tqdm seaborn matplotlib
