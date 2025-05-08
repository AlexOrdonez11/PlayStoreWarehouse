# 🧠 Sentiment Analysis & Emotional Clustering Pipeline (Databricks)

## Overview

This project implements a scalable pipeline scrapping data from Google Play Store and creating a Datawarehouse in SQL Server to analyze customer review sentiment, cluster both reviews and users by emotional patterns, and visualize key insights using Power BI.

Using Hugging Face's `roberta-base-go_emotions` model, the system extracts multi-label emotional sentiment from raw review text, then applies KMeans clustering to detect latent emotional groupings across both reviews and users. All processing is done in Databricks using Apache Spark for distributed scalability.

SQL Database MDF File = https://drive.google.com/file/d/14Fxz1u0bNp4IbxWUy_CXxz0RH_g1W0-y/view?usp=sharing

---

## 🔧 Tech Stack

- **google-play-scraper** — python script lib to get data 
- **Databricks (AWS)** — Batch processing, Delta Lake, Spark SQL
- **Hugging Face Transformers** — Pretrained RoBERTa model for emotion analysis
- **scikit-learn** — Clustering (KMeans), preprocessing
- **Power BI** — Dashboarding and visualization
- **SQL Warehouses** — Efficient Power BI data source
- **Visual Studio** — Cube creation for performance

---

## 📁 Project Structure

```plaintext
📦 project/
├── notebooks_python/
│   ├── Clustering_Reviews_Users.py             # Applies KMeans to emotion vectors (reviews/users)
│   ├── Sentiment_Analysis_Gathering_Store.py   # Processes 1M reviews in chunks using Hugging Face
│   ├── AppInfoTableLoading.py                  # Saves App information in a databricks workspace table
│   ├── ReviewsDataTableLoading.py              # Saves Review information in a databricks workspace table
│   ├── Local_SA.ipynb                          # Local trials on Sentiment Analysis and Clustering
│   ├── DBLoader.py                             # Creates structure to get data through google-play-scraper
│   ├── main.py                                 # Gets data from playstore and stores it in excel/csv flles

├── sql_visual_studio/
│   ├── AppInformationStagingTableLoadETL.zip   # Applies KMeans to emotion vectors (reviews/users)
│   ├── ReviewsETL.zip                          # Processes 1M reviews in chunks using Hugging Face
│   ├── FinalProject_Cube.sln                   # Visual Studio Cube to help performance        
│   ├── FinalProject_Report.sln                 # Visual Studio Report to dispkay KPIs

├── data/
│   ├── review...                               # Raw reviews
│   ├── AppData...                              # Apps metadata

├── pbix/
│   └── FinalProject_Visualizations.pbix        # Power BI reporting
│   └── SentimentAnalysis_PresentationDash.pbix # Power BI reporting

├── README.md                                   # This file


