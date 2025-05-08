# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
import math
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

df = spark.table("emotion_results")
df.display()

# COMMAND ----------

pdf = df.toPandas()
pdf

# COMMAND ----------

df_app = spark.table("appinfo")
df_app

# COMMAND ----------

df_reviews = spark.table("reviews")
df_reviews

# COMMAND ----------

pdf_reviews = df_reviews.toPandas()
pdf_app = df_app.toPandas()
pdf_app = pdf_app[['AppID','Genre_Name','GenreID']].drop_duplicates()
pdf_app

# COMMAND ----------

df = pd.merge(pdf_reviews, pdf_app, left_on="AppId", right_on="AppID", how="left").drop(['AppID','thumbsUpCount','at','content','AppId','GenreID'], axis=1)
df.head(5)

# COMMAND ----------

pdf_sentiment=pd.merge(pdf, df.drop(['userName'], axis=1), left_on="reviewId", right_on="reviewId", how="left")

# COMMAND ----------

pdf_sentiment

# COMMAND ----------

reviews_analized = pdf_sentiment.set_index('reviewId').drop('label', axis=1)
reviews_analized

# COMMAND ----------

reviews_dumm= pd.get_dummies(reviews_analized,columns=['Genre_Name'])
reviews_dumm

# COMMAND ----------

scaler = StandardScaler()
X_scaled = scaler.fit_transform(reviews_dumm)
X_scaled

# COMMAND ----------

k = 9
model = KMeans(n_clusters=k, random_state=42)
clusters = model.fit_predict(X_scaled)

clusters

# COMMAND ----------

reviews_dumm['cluster']=clusters
reviews_dumm

# COMMAND ----------

cluster_profiles = reviews_dumm.groupby('cluster').mean()
cluster_profiles

# COMMAND ----------

sentiment_profiles=cluster_profiles[reviews_analized.drop(['score', 'Genre_Name'], axis=1).columns]
sentiment_profiles

# COMMAND ----------

genre_profiles=cluster_profiles.filter(like='Genre_Name', axis=1)
genre_profiles

# COMMAND ----------

plt.figure(figsize=(14, 8))
sns.heatmap(sentiment_profiles.T, annot=True, cmap="coolwarm")
plt.title("Average Emotion Scores by Cluster")
plt.xlabel("Cluster")
plt.ylabel("Emotion")
plt.tight_layout()
plt.show()

# COMMAND ----------

plt.figure(figsize=(14, 10))
sns.heatmap(genre_profiles.T, annot=True, cmap="coolwarm")
plt.title("Average Genre Scores by Cluster")
plt.xlabel("Cluster")
plt.ylabel("Genre")
plt.tight_layout()
plt.show()

# COMMAND ----------

top_emotions_per_cluster = sentiment_profiles.apply(lambda row: row.sort_values(ascending=False).head(3).index.tolist(), axis=1)
top_emotions_per_cluster

# COMMAND ----------

top_genres_per_cluster = genre_profiles.apply(lambda row: row.sort_values(ascending=False).head(3).index.tolist(), axis=1)
top_genres_per_cluster

# COMMAND ----------

user_review=reviews_dumm.reset_index()
user_review=pd.merge(user_review, df.drop(['Genre_Name','score'], axis=1), left_on="reviewId", right_on="reviewId", how="left")
user_review=user_review.drop('reviewId', axis=1)
user_review

# COMMAND ----------

user_emotions = user_review.groupby('userName').mean()[reviews_analized.drop(['score', 'Genre_Name'], axis=1).columns.values]
user_emotions

# COMMAND ----------

user_genre = user_review.groupby('userName').sum().filter(like='Genre_Name')
user_genre_pct = user_genre.div(user_genre.sum(axis=1), axis=0)
user_genre_pct

# COMMAND ----------

user_final = pd.concat([user_emotions, user_genre_pct], axis=1)
user_final

# COMMAND ----------

X_user = scaler.fit_transform(user_final)

# COMMAND ----------

k = 9  # Or run elbow/silhouette stuff again
user_model = KMeans(n_clusters=k, random_state=42)
user_clusters = user_model.fit_predict(X_user)

user_final['user_cluster'] = user_clusters
user_final

# COMMAND ----------

cluster_users = user_final.groupby('user_cluster').mean()
cluster_users

# COMMAND ----------

sentiment_users=cluster_users[reviews_analized.drop(['score', 'Genre_Name'], axis=1).columns]
sentiment_users

# COMMAND ----------

genre_users=cluster_users.filter(like='Genre_Name', axis=1)
genre_users

# COMMAND ----------

plt.figure(figsize=(14, 8))
sns.heatmap(sentiment_users.T, annot=True, cmap="coolwarm")
plt.title("Average Emotion Scores by Cluster")
plt.xlabel("Cluster")
plt.ylabel("Emotion")
plt.tight_layout()
plt.show()

# COMMAND ----------

plt.figure(figsize=(14, 10))
sns.heatmap(genre_users.T, annot=True, cmap="coolwarm")
plt.title("Average Genre Scores by Cluster")
plt.xlabel("Cluster")
plt.ylabel("Genre")
plt.tight_layout()
plt.show()

# COMMAND ----------

top_userEmotions_per_cluster = sentiment_users.apply(lambda row: row.sort_values(ascending=False).head(3).index.tolist(), axis=1)
top_userEmotions_per_cluster

# COMMAND ----------

top_userGenres_per_cluster = genre_users.apply(lambda row: row.sort_values(ascending=False).head(3).index.tolist(), axis=1)
top_userGenres_per_cluster

# COMMAND ----------

user_tab=user_final.reset_index()[['userName','user_cluster']]
user_tab

# COMMAND ----------

spark_user_cluster = spark.createDataFrame(user_tab)
spark_user_cluster.write.format("delta").mode("overwrite").saveAsTable("user_cluster")

# COMMAND ----------

review_tab=reviews_dumm.reset_index()[['reviewId','cluster']]
review_tab

# COMMAND ----------

spark_review_cluster = spark.createDataFrame(review_tab)
spark_review_cluster.write.format("delta").mode("overwrite").saveAsTable("review_cluster")

# COMMAND ----------

user_sent=user_final[reviews_analized.drop(['score', 'Genre_Name'], axis=1).columns].reset_index()
user_sent

# COMMAND ----------

spark_user_sent = spark.createDataFrame(user_sent)
spark_user_sent.write.format("delta").mode("overwrite").saveAsTable("user_sentiments")

# COMMAND ----------




# COMMAND ----------

spark_reviews_clusters=spark.createDataFrame(cluster_profiles.reset_index().rename(columns={"Genre_Name_Art & Design": "Genre_Name_Art_Design", "Genre_Name_Auto & Vehicles": "Genre_Name_Auto_Vehicles","Genre_Name_Books & Reference":"Genre_Name_Books_Reference", "Genre_Name_Food & Drink": "Genre_Name_Food_Drink", "Genre_Name_Health & Fitness": "Genre_Name_Health_Fitness","Genre_Name_House & Home":"Genre_Name_House_Home", "Genre_Name_Libraries & Demo":"Genre_Name_Libraries_Demo", "Genre_Name_Maps & Navigation":"Genre_Name_Maps_Navigation","Genre_Name_Music & Audio":"Genre_Name_Music_Audio","Genre_Name_News & Magazines":"Genre_Name_News_Magazines","Genre_Name_Role Playing":"Genre_Name_Role_Playing", "Genre_Name_Travel & Local":"Genre_Name_Travel_Local","Genre_Name_Video Players & Editors":"Genre_Name_Video_Players_Editors"}))
spark_reviews_clusters.write.format("delta").mode("overwrite").saveAsTable("reviews_cluster_desc")

# COMMAND ----------

spark_users_clusters=spark.createDataFrame(cluster_users.reset_index().rename(columns={"Genre_Name_Art & Design": "Genre_Name_Art_Design", "Genre_Name_Auto & Vehicles": "Genre_Name_Auto_Vehicles","Genre_Name_Books & Reference":"Genre_Name_Books_Reference", "Genre_Name_Food & Drink": "Genre_Name_Food_Drink", "Genre_Name_Health & Fitness": "Genre_Name_Health_Fitness","Genre_Name_House & Home":"Genre_Name_House_Home", "Genre_Name_Libraries & Demo":"Genre_Name_Libraries_Demo", "Genre_Name_Maps & Navigation":"Genre_Name_Maps_Navigation","Genre_Name_Music & Audio":"Genre_Name_Music_Audio","Genre_Name_News & Magazines":"Genre_Name_News_Magazines","Genre_Name_Role Playing":"Genre_Name_Role_Playing", "Genre_Name_Travel & Local":"Genre_Name_Travel_Local","Genre_Name_Video Players & Editors":"Genre_Name_Video_Players_Editors"}))
spark_users_clusters.write.format("delta").mode("overwrite").saveAsTable("users_cluster_desc")