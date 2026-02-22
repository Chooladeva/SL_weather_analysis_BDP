# Sri Lanka Historical Weather Analysis & Evapotranspiration Prediction (2010–2024)

## Executive Summary

This project performs large-scale climate analytics on Sri Lanka’s historical weather data from 2010 to June 2024, leveraging distributed computing frameworks to uncover:

- Long-term climate trends
- District-level seasonal variations
- Extreme weather patterns
- Radiation and temperature anomalies

In addition to descriptive analytics, the project implements predictive modeling to identify weather conditions that lead to low evapotranspiration during May, supporting agricultural planning and environmental decision-making.

The system integrates Hadoop, Hive, and Spark for scalable processing and uses machine learning to model evapotranspiration behavior under varying meteorological conditions.

## Project Objectives
- Analyze 14+ years of district-level meteorological data
- Identify seasonal and extreme weather trends
- Compute radiation and temperature distribution metrics
- Model evapotranspiration behavior using machine learning
- Deliver decision-support insights for climate and agriculture stakeholders

## Dataset Overview

Geographic Scope
- Entire Sri Lanka
- All administrative districts

Data Coverage
- January 2010 – June 2024
- Daily weather observations

Core Variables
- Temperature (min, mean, max)
- Precipitation
- Wind speed
- Shortwave radiation
- Sunshine hours
- Evapotranspiration
- Geographic metadata (city, latitude, longitude, elevation)

The dataset spans over 14 years, enabling both longitudinal climate analysis and seasonal modeling.

## Big Data Processing Architecture

Due to dataset scale and computational complexity, distributed frameworks were used:
- Apache Hadoop (MapReduce)
- Apache Hive
- Apache Spark
- Apache Zeppelin

Each tool was applied based on analytical requirements.

## Analytical Components

**1️. Hadoop MapReduce Analysis**

MapReduce jobs computed:
- Total precipitation per district per month
- Mean temperature per district per month
- Month and year with highest recorded precipitation

These computations enabled detection of extreme rainfall years and spatial rainfall concentration patterns.

**2️. Apache Hive Queries**
- Hive was used for structured SQL-based climate analytics:
- Ranked top 10 most temperate cities (based on max temperature)
- Calculated average evapotranspiration for major agricultural seasons:
  - Maha Season: September – March
  - Yala Season: April – August

This provided district-level seasonal evapotranspiration profiles.

**3️. Apache Spark Analysis**

Spark was used for advanced distributed computation:
- Percentage of shortwave radiation > 15 MJ/m² per month
- Weekly maximum temperatures for hottest months
- Aggregated extreme weather metrics

Spark significantly improved computational efficiency for multi-year trend aggregation.

## Predictive Modeling — Low Evapotranspiration in May

Identify weather conditions associated with low evapotranspiration events in May, which directly impact:
- Irrigation planning
- Crop yield management
- Water resource allocation

## Machine Learning Workflow

Implemented using Spark MLlib:

- Data preprocessing & cleaning
- Feature selection and engineering
- Train-test split (80% / 20%)
- Model training
- Model validation and evaluation

## Model Evaluation & Visualization

Model performance evaluation charts were created using:

- Apache Zeppelin

Zeppelin was used to:
- Plot prediction vs actual evapotranspiration
- Visualize residual distributions
- Display regression performance metrics
- Analyze feature relationships interactively

This enabled clear, interpretable validation of model performance in a distributed Spark environment.

### Selected Features
- Precipitation hours
- Sunshine duration
- Wind speed
- Radiation
- Temperature

The model identified relationships between:
- Reduced sunshine
- Higher precipitation hours
- Increased humidity-related variables

And their collective impact on evapotranspiration decline.

## Visualization & Insights

Visual dashboards summarize:

- Most precipitous month per district
- Top 5 highest rainfall districts
- % of months with mean temperature > 30°C
- Extreme weather days (heavy rain + high wind)
- Seasonal evapotranspiration distribution

Visualization tools used:
- Tableau
- Static analytical dashboards

## Technology Stack

Big Data Processing
-  Apache Hadoop (MapReduce)
-  Apache Hive
-  Apache Spark

Machine Learning
- Spark MLlib

Programming
- Python
- PySpark

Visualization
- Tableau

Static dashboards

## Resources
- Tableau Dashboard

[Tableau Dashobaord Link](https://public.tableau.com/app/profile/chooladeva.lakshanaka.piyasiri/viz/SriLankaAnalyticsDashboard_17677487310780/Overview)

- Medium Articles

[Medium Article 1 Link](https://medium.com/@chooladevapiyasiri/harnessing-big-data-for-climate-insights-decoding-a-decade-of-weather-trends-across-sri-lanka-5d74f8ca4fb2)

[Medium Article 2 Link](https://medium.com/@chooladevapiyasiri/predicting-low-evapotranspiration-events-in-sri-lanka-using-apache-spark-mllib-9b4c18400cb0)

