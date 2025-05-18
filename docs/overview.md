{% docs __overview__ %}
# dbt Technical Training
*You may customize this page in the project's **docs/overview.md** file*

This project is provided as part of the dbt technical training.
It contains a layered model structure, along with snapshots, seeds, and macros.

Layers include:
- Staging (**stg**): Cleanse, filter data, and rename columns
- Intermediate (**int**): Join and aggregate stg models
- Mart (**mart**): Join data and define a star schema using a fact and dimensions for optimized reporting

[GitHub Project](https://github.com/dbt-snowflake-demo/sample-project)

{% enddocs %}