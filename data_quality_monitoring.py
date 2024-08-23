from pyspark.sql import SparkSession
from functools import reduce
import pandas as pd
from pyspark.sql.functions import col, mean, stddev, isnull, abs as pyspark_abs
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataQualityMonitoring") \
    .getOrCreate()

# Load data
df = spark.read.csv("/Users/Macbook Pro M1/Desktop/coding_learn/Python/Projects/ML Sorting by genre/spotify_data.csv", header=True, inferSchema=True)

# 1.Data validation 

# Define validation rules
non_null_columns = ['artist_name', 'track_name', 'track_id', 'popularity', 'year', 'genre']
valid_popularity_range = (0, 100)
valid_year_range = (1900, 2024)
valid_danceability_range = (0.0, 1.0)
valid_energy_range = (0.0, 1.0)
valid_loudness_range = (-60.0, 0.0)
valid_speechiness_range = (0.0, 1.0)
valid_acousticness_range = (0.0, 1.0)
valid_instrumentalness_range = (0.0, 1.0)
valid_liveness_range = (0.0, 1.0)
valid_valence_range = (0.0, 1.0)
valid_tempo_range = (0.0, 300.0)

# Check for null values
df_null_violations = df.filter(
    reduce(lambda a, b: a | b, [isnull(col(c)) for c in non_null_columns])
)

# Check for range violations
df_range_violations = df.filter(
    (col('popularity') < valid_popularity_range[0]) | (col('popularity') > valid_popularity_range[1]) |
    (col('year') < valid_year_range[0]) | (col('year') > valid_year_range[1]) |
    (col('danceability') < valid_danceability_range[0]) | (col('danceability') > valid_danceability_range[1]) |
    (col('energy') < valid_energy_range[0]) | (col('energy') > valid_energy_range[1]) |
    (col('loudness') < valid_loudness_range[0]) | (col('loudness') > valid_loudness_range[1]) |
    (col('speechiness') < valid_speechiness_range[0]) | (col('speechiness') > valid_speechiness_range[1]) |
    (col('acousticness') < valid_acousticness_range[0]) | (col('acousticness') > valid_acousticness_range[1]) |
    (col('instrumentalness') < valid_instrumentalness_range[0]) | (col('instrumentalness') > valid_instrumentalness_range[1]) |
    (col('liveness') < valid_liveness_range[0]) | (col('liveness') > valid_liveness_range[1]) |
    (col('valence') < valid_valence_range[0]) | (col('valence') > valid_valence_range[1]) |
    (col('tempo') < valid_tempo_range[0]) | (col('tempo') > valid_tempo_range[1])
)

# Log or handle validation violations
num_null_violations = df_null_violations.count()
num_range_violations = df_range_violations.count()
print(f"Number of null violations: {num_null_violations}")
print(f"Number of range violations: {num_range_violations}")

# Union the DataFrames for null violations and range violations
df_violations = df_null_violations.union(df_range_violations)

# Convert the Spark DataFrame to a Pandas DataFrame
pd_violations = df_violations.toPandas()

# Save the combined DataFrame to a CSV file using Pandas
output_csv_path = "/Users/Macbook Pro M1/Desktop/violations.csv"
pd_violations.to_csv(output_csv_path, index=False)

print(f"All violations saved to {output_csv_path}")

# 2. Data anomalies 

# Function to detect anomalies for a specific column
def detect_anomalies(column_name):
    # Calculate mean and standard deviation for the column
    stats = df.select(mean(col(column_name)).alias('mean'), stddev(col(column_name)).alias('stddev')).collect()
    mean_value = stats[0]['mean']
    stddev_value = stats[0]['stddev']
    threshold = 3 * stddev_value
    
    # Filter rows where the absolute deviation is greater than the threshold
    df_anomalies = df.filter(pyspark_abs(col(column_name) - mean_value) > threshold)
    return df_anomalies

# Detect anomalies for each column and combine them
anomaly_columns = ['danceability', 'energy', 'loudness', 'speechiness', 
                   'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']

# Initialize an empty DataFrame for all anomalies
df_all_anomalies = None

# Total number of rows for percentage calculation
total_rows = df.count()

# List to hold anomaly print details
anomaly_print_details = []

for col_name in anomaly_columns:
    anomalies = detect_anomalies(col_name)
    num_anomalies = anomalies.count()
    
    
    # Print the results
    print(f"Number of anomalies detected in {col_name}: {num_anomalies}")
    
    # Append details to the list for printing later
    anomaly_print_details.append({
        'Column': col_name,
        'Number of Anomalies': num_anomalies
    })

    # Combine anomalies into a single DataFrame
    if df_all_anomalies is None:
        df_all_anomalies = anomalies
    else:
        df_all_anomalies = df_all_anomalies.union(anomalies)

# Drop duplicates to avoid multiple occurrences of the same row if an anomaly is detected in multiple columns
df_all_anomalies = df_all_anomalies.dropDuplicates()

# Convert the combined DataFrame to a Pandas DataFrame
pd_all_anomalies = df_all_anomalies.toPandas()

# Save the Pandas DataFrame to a CSV file
output_csv_path = "/Users/Macbook Pro M1/Desktop/all_anomalies.csv"
pd_all_anomalies.to_csv(output_csv_path, index=False)

print(f"All anomalies saved to {output_csv_path}")


# 3. Alerts 

smtp_server = os.getenv("smtp_server")
smtp_port = os.getenv("smtp_port")
smtp_user = os.getenv("smtp_user")
smtp_password = os.getenv("smtp_password")

# Receiver email
receiver_email = 'anete.asafreja@gmail.com'


# Compilation of the report findings into a message
message_subject = "Data Quality Monitoring Report"
message_body = f"""Data Quality Monitoring has found the following issues:

- Number of null violations: {num_null_violations}
- Number of range violations: {num_range_violations}

Anomaly detection results:\n"""

for col_name in anomaly_columns:
    # Assuming num_anomalies is a variable you would have after anomaly detection for each column
    # You will need to fetch the actual number of anomalies detected for each column.
    # This is a placeholder loop for demonstration.
    num_anomalies = detect_anomalies(col_name).count()  # This should be replaced with actual counts
    message_body += f"- {col_name}: {num_anomalies} anomalies detected.\n"

# Setting up the email message
message = MIMEMultipart()
message["From"] = smtp_user
message["To"] = receiver_email
message["Subject"] = message_subject
message.attach(MIMEText(message_body, "plain"))

# File paths for the attachments
anomalies_csv_path = "/Users/Macbook Pro M1/Desktop/all_anomalies.csv"
violations_csv_path = "/Users/Macbook Pro M1/Desktop/violations.csv"

# Function to attach a file to the email
def attach_file(file_path, message):
    with open(file_path, "rb") as attachment:
        mime_base = MIMEBase("application", "octet-stream")
        mime_base.set_payload(attachment.read())
        encoders.encode_base64(mime_base)
        mime_base.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(file_path)}",
        )
        message.attach(mime_base)

# Attach both CSV files
attach_file(anomalies_csv_path, message)
attach_file(violations_csv_path, message)

# Sending the email
try:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Secure the connection
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, receiver_email, message.as_string())
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {str(e)}")