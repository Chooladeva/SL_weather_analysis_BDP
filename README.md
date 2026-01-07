# Sri Lanka Historical Weather Analysis & Evapotranspiration Prediction

This project analyses Sri Lanka’s historical weather data from **2010 to June 2024** to uncover climate patterns, extreme weather events, and district-level trends. It also implements predictive modeling to identify weather conditions leading to **low evapotranspiration in May**, providing insights for weather analysts and authorities.

---

## **Dataset**

- **Weather Observations:** Meteorological data for all districts, including temperature, precipitation, wind speed, radiation, and evapotranspiration.  
- **Location Details:** City names, latitudes, longitudes, and elevations, linked to the weather data via unique identifiers.  
- **Scope:** Full historical coverage over 14+ years, allowing analysis of trends, seasonal variations, and extreme events.

---

## **Project Overview**

### 1. Data Analysis

- **Hadoop MapReduce:**  
  - Calculated **total precipitation** and **mean temperature** per district per month.  
  - Identified the **month/year with highest precipitation**.  

- **Apache Hive:**  
  - Ranked **top 10 most temperate cities** using max temperature.  
  - Calculated **average evapotranspiration** for major agricultural seasons (Sep–Mar, Apr–Aug) by district.  

- **Spark:**  
  - Computed the **percentage of total shortwave radiation above 15MJ/m²** per month across districts.  
  - Derived **weekly maximum temperatures** for the hottest months.

---

### 2. Predictive Modeling

- Used **Spark MLlib** to model the relationship between precipitation hours, sunshine, and wind speed with evapotranspiration in May.  
- **Workflow:**  
  1. Data preprocessing and cleaning  
  2. Feature selection and engineering  
  3. Train-test split: 80% training, 20% validation  
  4. Model training, validation, and evaluation  
- Predicted weather conditions for low evapotranspiration to assist seasonal planning.

---

### 3. Visualization & Insights

- Created **static dashboards and visualizations** to summarize results:  
  - Most precipitous month/season per district  
  - Top 5 districts with highest precipitation  
  - Percentage of months with mean temperature >30°C  
  - Total number of extreme weather days (high precipitation + wind gusts)  

- Helps weather analysts and authorities make **data-driven decisions** for planning and mitigation.

---

## **Technologies Used**

- **Big Data & Processing:** Hadoop MapReduce, Apache Hive, Apache Spark  
- **Machine Learning:** Spark MLlib  
- **Visualization:** Tableau, static dashboards  
- **Programming:** Python, PySpark  

---

[Tableau Dashobaord Link](https://public.tableau.com/app/profile/chooladeva.lakshanaka.piyasiri/viz/SriLankaAnalyticsDashboard_17677487310780/Overview)

[Medium Article 1 Link](https://medium.com/@chooladevapiyasiri/harnessing-big-data-for-climate-insights-decoding-a-decade-of-weather-trends-across-sri-lanka-5d74f8ca4fb2)

[Medium Article 2 Link](https://medium.com/@chooladevapiyasiri/predicting-low-evapotranspiration-events-in-sri-lanka-using-apache-spark-mllib-9b4c18400cb0)

