# dbt Sample Project

This project transforms Snowflake sample data into a star schema structure (Fact + Dimensions) that is optimized for reporting and analytics.
Sample data includes customers, their orders, and order line items.

The project utilizes a layered model structure, along with snapshots, seeds, and macros.

Layers include:
- Staging (**stg**): Cleanse, filter data, and rename columns
- Intermediate (**int**): Join and aggregate stg models
- Mart (**mart**): Join data and define a star schema using a fact and dimensions for optimized reporting

**Lineage (DAG)**<br><br>
![dbt_sample_project_lineage](https://github.com/user-attachments/assets/6a6e12bc-0aa1-4a3e-940e-15246b25c247)

**Database structure**<br><br>
![snowflake_structure](https://github.com/user-attachments/assets/c91730f9-400d-4ba5-87b5-f9d9b805de27)