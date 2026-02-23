# Portfolio Projects

---
This repository is a portfolio of data analytics projects I've completed to serve as a showcase of my skills.

### [Etsy Shop Data Pipeline](https://github.com/jfox1620/Portfolio/blob/main/Etsy%20Shop%20Data%20Pipeline/README.md)

This project is an end-to-end ELT pipeline that extracts receipts, transactions, and fulfillment data from my Etsy shop and Gelato print-on-demand accounts via their APIs and loads the raw JSON into Amazon S3 using AWS Lambda. The data is automatically ingested into Snowflake using Snowpipe, where it is transformed with dbt into a structured, analytics-ready star schema consisting of fact and dimension tables. The pipeline follows modern cloud data engineering best practices: raw data is preserved in a partitioned S3 data lake, transformations occur inside the warehouse (ELT pattern), and data quality is enforced through dbt tests.

### [JSG Clickstream Analytics](https://github.com/jfox1620/Portfolio/blob/main/JSG%20Clickstream%20Analytics/README.md)

This project is a real-time data pipeline that captures anonymized user interactions on a nonprofit website and streams them through Apache Kafka for downstream processing. It demonstrates hybrid cloud architecture with serverless ingestion, NoSQL storage, and a cloud data warehouse, built as a production-ready system.

### [USGS Earthquakes Lakehouse Pipeline](https://github.com/jfox1620/Portfolio/blob/main/Earthquakes%20Lakehouse/README.md)

This project demonstrates a full production-grade lakehouse pipeline using PySpark and Delta Lake in Databricks. It ingests real-time earthquake data from the USGS API, lands the raw JSON into a structured Bronze-Silver-Gold architecture, and produces analytical tables suitable for dashboards, reporting, and machine learning. Also includes generation of an interactive visualization that displays earthquake locations over time.

### [AB109 Annual Evaluation Report](https://github.com/jfox1620/Portfolio/blob/main/Reports/2023-2024%20Annual%20Evaluation%20Report%20AB109.pdf)

The first comprehensive analytical evaluation I designed and developed for the San Joaquin Probation Department since taking over the assignment previously performed by a third-party vendor. This report delivers an in-depth statistical overview of the Public Safety Realignment (AB109) performance metrics in Probation.

### [Pretrial Release Decisions and Failure-to-Appear Analysis](https://github.com/jfox1620/Portfolio/tree/main/AB%20Test%20-%20Pretrial%20Court%20Appearance%20Rates%20by%20Judicial%20Release%20Decision)

I conducted an A/B-style analysis of pretrial release decisions using chi-squared testing to compare FTA rates, while cleaning and visualizing demographic and risk score data to reveal actionable policy insights.

+ [Report](https://github.com/jfox1620/Portfolio/blob/main/AB%20Test%20-%20Pretrial%20Court%20Appearance%20Rates%20by%20Judicial%20Release%20Decision/Pretrial%20Court%20Appearance%20Rates%20by%20Judicial%20Decision%20(2020%E2%80%932024).docx)
+ [Jupyter Notebook](https://github.com/jfox1620/Portfolio/blob/main/AB%20Test%20-%20Pretrial%20Court%20Appearance%20Rates%20by%20Judicial%20Release%20Decision/FTARatesABTest.ipynb)

### [Machine Learning Model - Probationer Success](https://github.com/jfox1620/Portfolio/blob/main/Machine%20Learning%20Model%20-%20Probationer%20Success/ProbationSuccessModeler.ipynb)

In this project I developed a machine learning model to predict probation outcomes using natural language processing on case notes, enabling identification of individuals at higher risk of non-compliance.

### [Open Powerlifting Exploratory Data Analysis](https://github.com/jfox1620/Portfolio/tree/main/Powerlifting%20Exploratory%20Analysis)

An analysis of up-to-date data on powerlifting competition meets. Data is pulled, processed, feature engineered, and visualized with Python in a Juptyer notebook. A Power BI report visualizes the cleaned data. For the Power BI report, both a PDF (for a quick view) and PBIX file (for download with the associated data) are included.

  + [Jupyter Notebook](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/Powerlifting_EDA.ipynb)
  + [Power BI Report](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/Open_Powerlifting_Insights.pdf)

### [Power BI Samples](https://github.com/jfox1620/Portfolio/tree/main/Power%20BI%20Samples)

This is a very small collection of some reports I have personally developed during my time at San Joaquin County Probation. This does not include any reports that I oversaw my team develop. More dashboards are viewable on the SJC Probation [official website](https://sjcprobation.org/probation-data-dashboards).

  + [Adult Recidivism](https://github.com/jfox1620/Portfolio/blob/main/Power%20BI%20Samples/Adult%20Recidivism.pdf)
  + [Adult Workload Stats](https://github.com/jfox1620/Portfolio/blob/main/Power%20BI%20Samples/Adult%20Workload%20Stats.pdf)
  + [Positions Report](https://github.com/jfox1620/Portfolio/blob/main/Power%20BI%20Samples/Positions%20Report.pdf)
  + [Pretrial Metrics & Trends](https://github.com/jfox1620/Portfolio/blob/main/Power%20BI%20Samples/Pretrial%20Metrics%20&%20Trends.pdf)

### [SQL Samples](https://github.com/jfox1620/Portfolio/tree/main/SQL%20Samples)

This is a small collection of various SQL scripts I wrote that serve to answer questions about data. CSV files are included for the video data scripts.

  + [Exploring Video Data](https://github.com/jfox1620/Portfolio/blob/main/SQL%20Samples/Exploring%20Video%20Data%20in%20SQL.sql)
  + [Monthly Customer Satisfaction](https://github.com/jfox1620/Portfolio/blob/main/SQL%20Samples/Monthly%20Customer%20Satisfaction%20SQL.docx)
  + [Invoice Totals by User](https://github.com/jfox1620/Portfolio/blob/main/SQL%20Samples/Invoice%20Totals%20by%20User%20SQL.docx)
