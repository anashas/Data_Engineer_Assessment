import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

import pyspark.sql.functions as F

def add_new_field(df, new_field, new_field_value, field_type):
    """
    Add a new field to the DataFrame with a specified field type.

    Args:
        df (DataFrame): Input DataFrame.
        new_field (str): Name of the new field to add.
        new_field_value (str): value of the new field
        field_type (str): Data type of the new field.

    Returns:
        DataFrame: DataFrame with the new field added.
    """
    # Add a new field with a default value of 'value' and cast it to the specified field type
    df = df.withColumn(new_field, F.lit(new_field_value).cast(field_type))
    return df

def update_field_type(df, field, new_field_type):
    """
    Update the data type of a field in the DataFrame.

    Args:
        df (DataFrame): Input DataFrame.
        field (str): Name of the field to update.
        new_field_type (str): New data type for the field.

    Returns:
        DataFrame: DataFrame with the updated field type.
    """
    # Cast the field to the new field type
    df = df.withColumn(field, F.col(field).cast(new_field_type))
    return df


def initialize_df(df):
    """
    Initialize a SparkDFDataset for validation.
    
    Args:
        df (DataFrame): Spark DataFrame to be validated.
        
    Returns:
        SparkDFDataset: Initialized SparkDFDataset for validation.
    """
    return SparkDFDataset(df)

def expect_table_column_count(df, min_value, max_value):
    """
    Expect the number of columns in the table to be within a range.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        min_value (int): Minimum expected number of columns.
        max_value (int): Maximum expected number of columns.
    """
    return df.expect_table_column_count_to_be_between(min_value=min_value, max_value=max_value)

def expect_table_row_count(df, min_value, max_value):
    """
    Expect the number of rows in the table to be within a range.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        min_value (int): Minimum expected number of rows.
        max_value (int): Maximum expected number of rows.
    """
    return df.expect_table_row_count_to_be_between(min_value=min_value, max_value=max_value)

def expect_column_existence(df, column_name):
    """
    Expect a specific column to exist in the DataFrame.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        column_name (str): Name of the column to check existence for.
    """
    return df.expect_column_to_exist(column_name)

def expect_ordered_column_list(df, column_list):
    """
    Expect columns to match an ordered list.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        column_list (list): Ordered list of column names.
    """
    return df.expect_table_columns_to_match_ordered_list(column_list)

def expect_column_value_range(df, column_name, min_value, max_value):
    """
    Expect values in a column to fall within a specified range.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        column_name (str): Name of the column to validate.
        min_value (int): Minimum expected value.
        max_value (int): Maximum expected value.
    """
    return df.expect_column_values_to_be_between(column_name, min_value=min_value, max_value=max_value)

def expect_column_min_max_range(df, column_name, min_value, max_value):
    """
    Expect the minimum and maximum values in a column to fall within a specified range.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        column_name (str): Name of the column to validate.
        min_value (int): Minimum expected value.
        max_value (int): Maximum expected value.
    """
    df.expect_column_min_to_be_between(column_name, min_value=min_value, max_value=max_value)
    return df.expect_column_max_to_be_between(column_name, min_value=min_value, max_value=max_value)

def expect_unique_column_values(df, column_name):
    """
    Expect values in a column to be unique.
    
    Args:
        df (SparkDFDataset): SparkDFDataset object.
        column_name (str): Name of the column to validate uniqueness.
    """
    return df.expect_column_values_to_be_unique(column_name)


def show_expectation_results(expectation_results):
    """
    Write the results of expectation tests to a DataFrame and save it to disk using PySpark.

    Args:
        expectation_results (dict): Dictionary containing success status of expectation tests.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Write Expectation Results") \
        .getOrCreate()

    # Map True/False to Passed/Failed
    status_mapping = {True: 'Passed', False: 'Failed'}

    # Convert dictionary to list of Rows, with status mapped using status_mapping
    rows = [Row(Expectation=key, Status=status_mapping[value]) for key, value in expectation_results.items()]

    # Create DataFrame from the list of Rows
    df = spark.createDataFrame(rows)

    return df
