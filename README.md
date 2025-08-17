# ✈ Airlines Data Engineering & Analysis Project

## 📌 Overview
This project combines Data Engineering and Data Analysis to demonstrate skills in building data pipelines, performing data cleaning, and conducting exploratory analysis.
The dataset includes information on airlines, airports, aircraft, and flight routes, which are used to answer key business questions and visualize industry trends.

**Dataset available on [Kaggle](https://www.kaggle.com/datasets/rohitgrewal/airlines-flights-data/data)**

Workflow

1. Ingestion → Dataset download from Kaggle using the API.
2. Processing → Data cleaning and transformation with PySpark (Bronze and Silver tables).
3. Analysis → Exploratory analysis and visualizations with Pandas and Matplotlib (Gold layer).

---

## 📂  Project Structure
        Airlines_project/
        │
        ├── Data/
        │ ├── raw/ # Raw downloaded data
        │ ├── clean/ # Cleaned data in Parquet format
        │ └── data_fetch.py # Ingestion script
        │
        ├── Load/
        │ └── pyspark_pipeline.py # Data cleaning & transformation (Bronze & Silver layers)
        │
        ├── Analyze/
        │ └── gold_notebook.ipynb # Exploratory analysis & visualization (Gold layer)
        │
        ├── README.md # Project documentation


---

## ⚙️ Tech Used
- **Python 3.x**
- **PySpark**
- **Pandas**
- **Matplotlib** 
- **Parquet y CSV** 
- **Kaggle API** 

---

## 📊 Business Questions 
1. **Which are the airlines in the dataset, how many flights each one has?**  
2. **Show Bar Graphs representing the Departure Time & Arrival Time.**  
3. **Show Bar Graphs representing the Source City & Destination City.**  
4. **Does price varies with airlines?**  
5. **Does ticket price change based on the departure time and arrival time?**  
6. **How the price changes with change in Source and Destination?**  
7. **How is the price affected when tickets are bought in just 1 or 2 days before departure?**  
8. **How does the ticket price vary between Economy and Business class?**  
9. **What will be the Average Price of Vistara airline for a flight from Delhi to Hyderabad in Business Class?**  

---


