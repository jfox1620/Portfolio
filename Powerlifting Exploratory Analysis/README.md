# Powerlifting Data Analysis & Visualization

This project explores historical powerlifting competition results using the OpenPowerlifting dataset. The goal is to analyze trends in athlete performance, competition participation, and factors contributing to higher lifts across squat, bench press, and deadlift.  

## Project Overview
- Extracts competition data from the OpenPowerlifting dataset via direct download.  
- Performs preprocessing, data cleaning, and feature engineering for analysis.  
- Saves transformed datasets as Parquet files (`PL_Meets_Processed.parquet`, `PL_Participants_Processed.parquet`) for efficient storage and downstream analysis.  
- Conducts exploratory data analysis (EDA) using Python (Pandas, NumPy, Seaborn, Matplotlib).  
- Generates interactive visualizations and dashboards in Power BI to communicate insights.  

## Repository Contents
- `[Powerlifting EDA Notebook](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/Powerlifting_EDA.ipynb)` — Jupyter Notebook with full analysis and visualizations  
- `[Power BI Report](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/Open_Powerlifting_Insights.pdf)` — Sample dashboard screenshot  
- `[PL_Meets_Processed parquet file](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/PL_Meets_Processed.parquet)` — Cleaned & transformed meets dataset  
- `[PL_Participants_Processed parquet file](https://github.com/jfox1620/Portfolio/blob/main/Powerlifting%20Exploratory%20Analysis/PL_Participants_Processed.parquet)` — Cleaned & transformed participants dataset  

## Dataset
- Source: [OpenPowerlifting Project](https://www.openpowerlifting.org/)  
- Dataset includes historical meet results, athlete attributes, and lift records dating back to the 1960s.  
- Limitations: Some fields may be missing depending on federation reporting; data reflects only competition results, not individual training.  

## Key Findings
- Lift performance is positively correlated with sex, age, bodyweight, and use of equipment.  
- Powerlifting participation has grown significantly over the last decade, with increasing diversity in competitors.  
- Most competitions take place in the USA, with untested meets showing higher lift totals.  

## Technologies Used
- Python, Pandas, NumPy, Seaborn, Matplotlib, Jupyter Notebook  
- Power BI for interactive visualizations  
- Parquet file format for transformed datasets  

## Future Work
- Incorporate additional datasets for more comprehensive analysis  
- Explore advanced performance metrics (Wilks, Dots, Goodlift points)  
- Investigate lift attempt patterns and success rates across competitions
