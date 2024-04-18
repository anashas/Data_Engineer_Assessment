# Dynamic Schema Evolution in PySpark Pipeline

## Design Solution

To manage schema evolution dynamically in our data pipelines, we propose the following solution:

1. **Schema Tracking**: Maintain a schema store to track different versions of the schema. This schema store will contain mappings of schema versions to their respective schemas.

2. **Schema Evolution Functions**: Implement functions to handle schema evolution tasks, such as adding new fields and updating existing field types. These functions should be designed to operate on PySpark DataFrames and modify their schemas accordingly.

3. **Dynamic Schema Inference**: Utilize PySpark's dynamic schema inference capabilities while reading data from sources. This allows us to infer the schema of incoming data without explicitly defining it.

4. **Versioning**: Keep track of different versions of the schema to ensure traceability and manage schema changes over time.

## PySpark Pipeline Implementation

### Use Cases Considered

1. **Addition of New Data Fields**: When new fields are introduced in the source records, our pipeline should be able to accommodate these changes dynamically.

2. **Update of Existing Field Data Types**: If the data type of an existing field changes in the source records, our pipeline should handle this evolution seamlessly.

# Data Quality Validation in PySpark Pipeline

## Design Solution

To address the data quality validation requirements, we propose the following design solution:

1. **Selection of Data Quality Framework**: Utilize an open-source data quality framework compatible with PySpark to facilitate comprehensive data validation and profiling. We choose great-expectations due to its robust features and support for PySpark integration.

2. **Expectation Definitions**: Define a set of data quality expectations based on the characteristics of the dataset and business requirements. These expectations include criteria such as column count, row count, column existence, value ranges, and uniqueness.

3. **Implementation of Expectations**: Develop PySpark functions to validate the defined expectations against the dataset. These functions leverage PySpark's capabilities for efficient data processing and validation.

4. **Reporting and Visualization**: Generate a quality report summarizing the results of data quality validation after each pipeline run. The report should provide insights into the overall quality of the dataset and highlight any issues or anomalies detected.

## PySpark Pipeline Implementation

### Framework Integration

We integrate great-expectations into our PySpark pipeline to leverage its data quality validation features.

### Expectation Definitions

- **Table Column Count**: Ensure the dataset has a specified range of columns.
- **Table Row Count**: Validate the number of rows falls within a specified range.
- **Column Existence**: Check for the existence of specific columns in the dataset.
- **Ordered Column List**: Verify the order of columns against a predefined list.
- **Column Value Range**: Validate the range of values for a particular column.
- **Column Min/Max Range**: Check if column values fall within specified minimum and maximum ranges.
- **Unique Column Values**: Ensure uniqueness of values within a column.

### Quality Report Structure

The quality report includes a summary of expectation results, indicating the success or failure of each validation criteria. Additionally, detailed insights may be provided for failed expectations to assist in identifying and resolving data quality issues.

# Scalable Data Model for Taxi Service Domain

## Data Model Entities

### Users
- UserID
- UserType
- Name
- Email
- Phone

### Trips
- TripID
- UserID (Passenger)
- DriverID
- VehicleID
- StartLocationID
- EndLocationID
- StartTime
- EndTime
- Fare
- Distance

### Vehicles
- VehicleID
- DriverID
- Model
- Registration

### Locations
- LocationID
- Name
- Coordinates
- Address

## Relationships
- Users to Trips: Each trip involves a user as a passenger and a driver.
- Vehicles to Trips: Each trip is associated with a vehicle.
- Locations to Trips: Start and end locations of each trip.
- Users to Vehicles: Each driver is associated with one or more vehicles.

## Physical Data Model

- **Users**: User information
- **Trips**: Trip details
- **Vehicles**: Vehicle information
- **Locations**: Location details

## Key Performance Indicators (KPIs)

1. **Average trip distance per day/week/month**
2. **Total revenue generated per driver**
3. **Busiest times of the day/week/month for trips**
4. **Average waiting time for passengers**
5. **Most frequently visited locations**
6. **Percentage of completed trips vs. canceled trips**
7. **Driver utilization rate**

- The data used for testing and demonstration purposes is sourced from the [Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail) available at UCI Irvine Machine Learning Repository.








