import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from scipy.stats import zscore
import boto3
import io
from sqlalchemy import create_engine, Integer, String, Float, Date
import mysql.connector
from luigi import Task, LocalTarget, build
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt

# AWS and RDS configuration
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
BUCKET_NAME = 'capstone-group15'
S3_KEY = 'Sports_Dataset_1M.csv'

MYSQL_HOST = 'sports-capstone.criyc0u6cx0c.eu-north-1.rds.amazonaws.com'
MYSQL_PORT = 3306
MYSQL_DB = 'sports'
MYSQL_USER = ''
MYSQL_PASSWORD = ''

# Luigi Task 1: Download data from S3
class DownloadData(Task):
    def output(self):
        return LocalTarget("data.csv")

    def run(self):
        # Download file from S3
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=S3_KEY)
        file_size = obj['ContentLength']
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Loading CSV") as pbar:
            # Wrap the file-like object with a progress bar
            stream = io.BytesIO()
            for chunk in obj['Body']:
                stream.write(chunk)
                pbar.update(len(chunk))
            stream.seek(0)  # Reset the pointer to the beginning

        # Load the CSV into a DataFrame
        df = pd.read_csv(stream)
        df.to_csv(self.output().path, index=False)
        print("CSV loaded successfully.")

# Luigi Task 2: Explore Data
class ExploreData(Task):
    def requires(self):
        return DownloadData()

    def output(self):
        return LocalTarget("exploration_report.txt")

    def run(self):
        df = pd.read_csv(self.input().path)
        missing_columns = df.columns[df.isnull().any()]
        
        # Generate Exploration Report
        with self.output().open('w') as f:
            f.write("Dataset Info:\n")
            f.write(str(df.info()) + "\n\n")
            f.write("Summary Statistics:\n")
            f.write(str(df.describe(include='all')) + "\n\n")
            f.write("Missing Values Count:\n")
            f.write(str(df.isnull().sum()) + "\n\n")
            f.write(f"Columns with Missing Values: {missing_columns}\n")
            for col in df.columns:
                if 'Date' in col:
                    f.write(f"{col} should be converted to datetime.\n")
                elif 'ID' in col:
                    f.write(f"{col} should be converted to integer.\n")
        
        # Visualize distributions of missing columns
        for col in missing_columns:
            plt.figure(figsize=(8, 6))
            sns.histplot(df[col], kde=True, bins=30, color='blue', alpha=0.7)
            plt.title(f"Distribution of {col} (Including Missing Values)")
            plt.xlabel(col)
            plt.ylabel("Frequency")
            plt.grid(axis='y')
            plt.savefig(f"{col}_distribution.png")
            plt.close()

# Luigi Task 3: Clean and Validate Data
class CleanData(Task):
    def requires(self):
        return DownloadData()

    def output(self):
        return LocalTarget("cleaned_data.csv")

    def run(self):
        try:
            # Read CSV with error handling
            df = pd.read_csv(self.input().path, on_bad_lines='skip')  # Skip malformed lines

            # Handle Missing Values for Numeric Columns
            columns_to_impute = ['Attendance', 'Shots_On_Target_Team1', 'Shots_On_Target_Team2', 'Goals', 'Assists']
            mean_imputer = SimpleImputer(strategy='mean')
            df[columns_to_impute] = mean_imputer.fit_transform(df[columns_to_impute])

            # Handle Missing Values for Categorical Columns
            categorical_imputer = SimpleImputer(strategy='most_frequent')
            df['Weather'] = categorical_imputer.fit_transform(df[['Weather']]).ravel()

            # Detect and Correct Anomalies
            numeric_cols = df.select_dtypes(include=[np.number])
            z_scores = zscore(numeric_cols, nan_policy='omit')
            abs_z_scores = np.abs(z_scores)
            anomalies = (abs_z_scores > 3).any(axis=1)
            df_cleaned = df[~anomalies]

            Q1 = numeric_cols.quantile(0.25)
            Q3 = numeric_cols.quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((numeric_cols < (Q1 - 1.5 * IQR)) | (numeric_cols > (Q3 + 1.5 * IQR))).any(axis=1)
            df_cleaned = df_cleaned[~outliers]

            # Save cleaned data
            df_cleaned.to_csv(self.output().path, index=False)
        except Exception as e:
            print(f"Error in CleanData task: {e}")

# Luigi Task 4: Load Data into RDS
class LoadToRDS(Task):
    def requires(self):
        return CleanData()

    def output(self):
        return LocalTarget("rds_load_success.txt")

    def run(self):
        # Read the cleaned data from the input file
        df = pd.read_csv('cleaned_data.csv')
        
        # Define the MySQL connection string
        engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")


        # Write the DataFrame to the RDS database
        df.to_sql(
            "sports_data",
            con=engine,
            if_exists="replace",  # Use 'replace' to overwrite the table, or 'append' to add rows
            index=False
        )

        # Confirm success
        with self.output().open('w') as f:
            f.write("RDS Load Successful")

# Luigi Task 5: Query Data from RDS
class QueryRDS(Task):
    def requires(self):
        return LoadToRDS()

    def output(self):
        return LocalTarget("query_results.txt")

    def run(self):
        # Establish connection to MySQL
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = conn.cursor()

        # Query Example
        query = """
        SELECT Weather, AVG(Attendance) AS Avg_Attendance
        FROM sports_data
        GROUP BY Weather;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Close the connection
        conn.close()

        # Write results to output file
        with self.output().open('w') as f:
            for row in results:
                f.write(f"{row}\n")


# Execute the Luigi Pipeline
if __name__ == "__main__":
    build([QueryRDS()], local_scheduler=True)
