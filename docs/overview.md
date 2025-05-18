{% docs __overview__ %}
![CGI](assets/cgi_logo.png)
# dbt Technical Training
*You may customize this page in the project's **docs/overview.md** file*

This dbt project is provided as part of CGI's dbt technical training.
It contains a layered model structure, including snapshots, seeds, and macros.

Layers include:
- Staging (**stg**): Cleanse, filter data, and rename columns
- Intermediate (**int**): Join and aggregate stg models
- Mart (**mart**): Join data and define a star schema using a fact and dimensions for optimized reporting

[GitHub Project](https://github.com/dbt-snowflake-demo/sample-project)
[cgi.com](https://www.cgi.com/en)

{% enddocs %}