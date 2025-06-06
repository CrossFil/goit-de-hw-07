#  Airflow DAG: Olympic Medal Counter

This assignment focuses on building an Apache Airflow DAG that simulates counting Olympic medals from a dataset and inserting the results into a MySQL table. The pipeline includes dynamic branching, conditional logic, delay simulation, and validation with a sensor.

---

##  Goal

Create an Airflow DAG that:

- Creates a MySQL table if it doesn't exist
- Randomly selects one of three medal types
- Branches into one of three tasks based on the medal type
- Counts medal occurrences in a dataset and inserts the result
- Introduces a delay
- Uses a sensor to validate that the most recent record is not older than 30 seconds

---
