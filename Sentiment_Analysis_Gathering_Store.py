# Databricks notebook source
# MAGIC %pip install transformers scikit-learn nltk

# COMMAND ----------

pip install matplotlib seaborn

# COMMAND ----------

from transformers import pipeline, AutoTokenizer
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import math
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

# COMMAND ----------

df_reviews = spark.table("reviews")
df_reviews

# COMMAND ----------

df_app = spark.table("appinfo")
df_app

# COMMAND ----------

pdf_reviews = df_reviews.toPandas()
pdf_app = df_app.toPandas()

# COMMAND ----------

pdf_app

# COMMAND ----------

pdf_app = pdf_app[['AppID','Genre_Name','GenreID']].drop_duplicates()
pdf_app

# COMMAND ----------

df = pd.merge(pdf_reviews, pdf_app, left_on="AppId", right_on="AppID", how="left")
df.head(5)

# COMMAND ----------

sdf = spark.createDataFrame(df)

# COMMAND ----------

total = sdf.count()
batch_size = 10000
num_batches = math.ceil(total / batch_size)

print(f"Processing {total} rows in {num_batches} batches")


# COMMAND ----------

window = Window.orderBy(lit(1))  # just gives a dumb order
sdf = sdf.withColumn("row_id", row_number().over(window))

# COMMAND ----------

model_name = "SamLowe/roberta-base-go_emotions"
tokenizer = AutoTokenizer.from_pretrained(model_name)
emotion_classifier = pipeline(
    "text-classification",
    model=model_name,
    return_all_scores=True
)

# COMMAND ----------

def classify_safe(text, max_tokens=51):
    try:
        if text == None:
            text=""

        # Step 1: tokenize & truncate
        tokens = tokenizer.encode(text, truncation=True, max_length=max_tokens)
        
        # Step 2: decode back to string
        truncated_text = tokenizer.decode(tokens, skip_special_tokens=True)

        # Step 3: classify the safe, model-friendly text
        results = emotion_classifier(truncated_text)
        
        sorted_results = sorted(results[0], key=lambda x: x['score'], reverse=True)
        top_label = sorted_results[0]['label']
        scores = {r['label']: r['score'] for r in sorted_results}
        scores['label'] = top_label
        return scores
    except Exception as e:
        return {"label": "error", "error": str(e)}

# COMMAND ----------

for i in range(104, 109):

    print(f"Batch {i+1}: Expected row_id range {i*batch_size} - {(i+1)*batch_size - 1}")
    print(f"\nProcessing batch {i+1} / {num_batches}")

    # Slice the batch manually using row_id
    df_batch = sdf.filter((col("row_id") >= i * batch_size) & (col("row_id") < (i + 1) * batch_size)) \
                   .drop("row_id")

    # Remove already processed rows if table exists
    if spark._jsparkSession.catalog().tableExists("emotion_results"):
        processed_ids = spark.table("emotion_results").select("reviewId").distinct()
        df_batch = df_batch.join(processed_ids, on="reviewId", how="left_anti")

    count_to_process = df_batch.count()
    print(f"Batch {i+1}: {count_to_process} new reviews to process")

    if count_to_process == 0:
        continue

    # Convert to pandas
    pdf = df_batch.select("reviewId", "content").toPandas()

    results = []
    for idx, row in pdf.iterrows():
        result = classify_safe(row['content'])
        result.update({'reviewId': row['reviewId']})
        results.append(result)

    emotion_df = pd.DataFrame(results)
    spark_emotions = spark.createDataFrame(emotion_df)

    # Save or append to target Delta table
    spark_emotions.write.format("delta").mode("append").saveAsTable("emotion_results")

print("\nAll batches processed. Emotional damage extracted.")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

