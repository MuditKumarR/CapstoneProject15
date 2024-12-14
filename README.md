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
```
# Configuration

## AWS Credentials

You need to configure the following parameters to allow the pipeline to download data from S3:

- **AWS Access Key**: Set your AWS access key.
- **AWS Secret Key**: Set your AWS secret key.

## MySQL RDS Configuration

Provide the appropriate values for the following MySQL RDS parameters:

- **MYSQL_HOST**: The endpoint of your RDS instance.
- **MYSQL_PORT**: The port for MySQL (default is 3306).
- **MYSQL_DB**: The database name where the data will be stored.
- **MYSQL_USER**: The username for MySQL.
- **MYSQL_PASSWORD**: The password for the MySQL user.

Ensure that the MySQL instance is accessible and that the database and table are correctly configured.


# Getting AWS Access Keys

To interact with AWS services (like S3), you'll need AWS Access Keys. These keys allow your application to authenticate and interact with AWS resources.

### Steps to Obtain AWS Access Keys:

1. **Sign in to AWS Console**: Go to the AWS Management Console and log in with your AWS account.
   
2. **Access IAM**: In the console, navigate to IAM (Identity and Access Management).
   
3. **Create a New User**:
   - In the IAM dashboard, click **Users** on the left-hand menu, then click **Add user**.
   - Choose **Programmatic access** to allow API access.

4. **Attach Policies**: Choose the permissions for the user:
   - Attach the policy **AmazonS3ReadOnlyAccess** to allow read access to S3.

5. **Create User**: After completing the steps, you will receive your **Access Key ID** and **Secret Access Key**. Save these securely, as they will not be shown again.

6. Once you have the keys, use them to configure the pipeline.

---

# Creating a MySQL RDS Instance

To create an Amazon RDS instance for MySQL, follow these steps:

### Steps to Create MySQL RDS Instance:

1. **Sign in to AWS Console**: Log in to the AWS Management Console.

2. **Navigate to RDS**: In the search bar, type **RDS** and click on the **RDS service**.

3. **Launch a DB Instance**:
   - Click on **Create database**.
   - Choose the **MySQL** engine.
   - Select the **Free Tier** option if you are eligible.
   - Fill in the details, such as DB instance identifier, master username, and password. Remember to note the **Master Username** and **Password** as you'll need these for connection.

4. **Configure VPC and Security Group**:
   - Ensure your RDS instance is accessible. Create or choose an existing VPC and Security Group that allows inbound traffic on port **3306** (MySQL default).
   - Set the **Public accessibility** to **Yes** if you need external access.

5. **Launch the Instance**: After configuring, click **Create database**.

6. **Obtain RDS Endpoint**: Once the instance is available, find its endpoint in the RDS dashboard. This will be used as the `MYSQL_HOST` in your configuration.

After creating the RDS instance, make sure the instance is publicly accessible if you're connecting from your local machine, or configure the VPC to allow access from specific sources.

---

# How to Run

To execute the entire pipeline, simply run the Python script:

```bash
python sports_data_pipeline.py
```
This will execute the pipeline using **Luigi's local scheduler**, running each task in sequence and displaying their progress. After the tasks are completed, the results will be saved in the respective output files.

## Task Breakdown:

- **Download Data from S3**: This task downloads the dataset from an S3 bucket and stores it locally.
- **Explore Data and Generate Report**: This task explores the dataset, generates summary statistics, and identifies missing values.
- **Clean the Data and Handle Missing Values**: This task handles missing data by imputing values and removing anomalies and outliers.
- **Load Cleaned Data to MySQL RDS**: This task loads the cleaned data into a MySQL RDS database.
- **Query Data from MySQL RDS**: This task executes a query on the MySQL RDS instance and stores the results in a text file.

Each task creates an output file (e.g., CSVs, text files) that is stored locally in the directory from which the script is run.
