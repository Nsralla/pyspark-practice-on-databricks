# Databricks notebook source
# MAGIC %md
# MAGIC ### Read CSV data
# MAGIC

# COMMAND ----------

df = spark.read.csv("dbfs:/Volumes/workspace/default/my_volume/BigMart Sales.csv", header=True, inferSchema=True)
df.show()

# spark : is the session, created auto
# header: tells spark to display columns names at first
# inferSchema: tells spark to detect columns data types, otherwise all columns are treated as strings

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ JSON data format

# COMMAND ----------

df = spark.read.json("dbfs:/Volumes/workspace/default/my_volume/drivers.json")
df.display()
# no inferSchema here
# you CAN add multiLine=True if your json data  looks like this:

# json
# Copy
# Edit
# {
#   "name": "Ali",
#   "age": 30
# }
# i.e., one object over multiple lines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

schema = """
    Item_Identifier STRING,
    Item_Weight STRING,
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
"""

# Item weight changed to string

# COMMAND ----------

df = spark.read.schema(schema).csv("dbfs:/Volumes/workspace/default/my_volume/BigMart Sales.csv",header=True)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT STATEMENT
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

df.select(col("Item_Identifier"), col("Item_Type")).display()

# COMMAND ----------

df.select(col("Item_Outlet_Sales").alias("sales")).display()

# COMMAND ----------

df.select((col("Item_MRP")*1.1).alias("price with tax")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("Item_Weight") > 10).display()


# COMMAND ----------

df.select(col("Item_Weight")).filter(col("Item_Weight") > 10).display()
# chaining


# COMMAND ----------

# and condition
df.filter((col("Item_Weight") > 10) & (col("Item_Type") == "Dairy")).display()

# COMMAND ----------

# or condition
df.filter((col("Item_Weight") > 10) | (col("Item_Type") == "Dairy")).display()

# COMMAND ----------

# not condition
df.filter(~(col("Item_Weight") == 10)).display()

# COMMAND ----------

# filter by membership
df.filter((col("Outlet_Size").isin("Small","Medium"))).display()

# COMMAND ----------

df.filter(col("Item_Weight") > 10).filter(col("Item_MRP") < 250).show()


# COMMAND ----------

df.filter((col("Outlet_Size").isNull()) & (col("Outlet_Location_Type").isin( "Tier 1","Tier 2"))).display()

# COMMAND ----------

df.select("Outlet_Location_Type", "Outlet_Size").distinct().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename one or more columns in your DataFrame 

# COMMAND ----------

df = df.withColumnRenamed("Item_Identifier", "id")
# This returns a new DataFrame — Spark DataFrames are immutable.

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumnRenamed("Item_Fat_Content","Item_Fat") \
    .withColumnRenamed("Item_Type","type") 
df.display()
       
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn()

# COMMAND ----------

# MAGIC %md
# MAGIC #### It's very flexible — you can:
# MAGIC
# MAGIC Add computed columns (e.g., tax = price × 0.1)
# MAGIC
# MAGIC Replace values
# MAGIC
# MAGIC Convert data types
# MAGIC
# MAGIC Extract or transform strings, dates, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Syntax
# MAGIC ****
# MAGIC df2 = df.withColumn("new_column_name", expression_or_transformation)
# MAGIC
# MAGIC
# MAGIC
# MAGIC Like most PySpark methods, it returns a new DataFrame.

# COMMAND ----------

# 1. Add New Column (e.g. tax = 10% of MRP):
df = df.withColumn("tax",col("Item_MRP") * 0.1)
df.display()

# COMMAND ----------

# 2. Modify Existing Column (e.g. double the weight):
df = df.withColumn("Item_Weight", col("Item_Weight") * 2)
df.display()

# COMMAND ----------

# Convert Column Type (string → double):
df = df.withColumn("Item_Weight", col("Item_weight").cast("double"))
df.printSchema()

# COMMAND ----------

# Add a Constant Column:
from pyspark.sql.functions import lit
df = df.withColumn("Country",lit("Palestine"))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn("Item_Fat", regexp_replace(col("Item_Fat"), "Regular", "Reg"))
display(df)

# COMMAND ----------

df.withColumn("Item_Weight", col("Item_Weight").cast("double")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SORT
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  Basic Syntax
# MAGIC
# MAGIC df.sort("column_name").show()
# MAGIC
# MAGIC This sorts the DataFrame by the specified column in ascending order (default).

# COMMAND ----------

df.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

df.sort(col("Item_Weight").asc()).display()

# COMMAND ----------

df.sort(col("Item_Weight").desc(), col("Item_MRP").asc()).display()

# COMMAND ----------

df.sort(col("Item_Weight").desc(), col("Item_MRP").asc()).limit(8).display()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #### drop() is used to remove columns from a DataFrame, or (in some contexts) remove rows that contain null values.

# COMMAND ----------

# drop one column
df = df.drop("Country")
# drop multi columns
df = df.drop("Item_Visibility", "tax")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop Rows with Nulls

# COMMAND ----------

df = df.dropna()
# This removes all rows that have any null values.

# You can also drop only if specific columns are null:
df = df.dropna(subset=["Item_Weight","Item_MRP"])
df.display()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### dropDuplicates() removes duplicate rows based on:
# MAGIC
# MAGIC all columns (default), or
# MAGIC
# MAGIC a subset of columns you specify.

# COMMAND ----------

df = df.dropDuplicates()

# Removes rows that are completely identical across all columns.

# COMMAND ----------

df.display()

# COMMAND ----------

df2 = df.dropDuplicates(["id"])
df2.display()
# Keeps only one row per unique Item_Identifier, and removes all other duplicates (even if other columns differ).

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION
# MAGIC

# COMMAND ----------

df1 = df.filter(col("Item_Weight") > 5)
df2 = df.filter(col("Item_weight") > 10)
df3 = df2.union(df2)
# df3 = df3.dropDuplicates()
df3.display()
# df1 and df2 must have exactly the same schema (same number of columns, same names, same types, and order)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union by name
# MAGIC

# COMMAND ----------

df3 = df1.unionByName(df2)
df3.display()
# it makes sure columns are within the same order, even if not, it will make them

# COMMAND ----------

df1 = [(1,'nsr'),(2,'ali')]
schema1 = 'id int, name string '
df1 = spark.createDataFrame(df1,schema1)


df2 = [(3,'mo'),(4,'bro')]
df2 = spark.createDataFrame(df2,schema1)


# COMMAND ----------

df3 = df1.union(df2)
df3.display()

# COMMAND ----------

df1 = [(1,'nsr'),(2,'ali')]
schema1 = 'id int, name string '
df1 = spark.createDataFrame(df1,schema1)


df2 = [('mo',3),('bro',4)]
schema2 = 'name string, id int'
df2 = spark.createDataFrame(df2,schema2)


# COMMAND ----------

# df1.unionAll(df2).display() error
df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### All used with from pyspark.sql.functions import *
# MAGIC (You’ll also usually combine them with col("column_name") inside select() or withColumn())

# COMMAND ----------

from pyspark.sql.functions import initcap, col
df.select(initcap(col("Item_Type"))).display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(upper(col("Item_Type"))).display()

# COMMAND ----------

df.select(col("Item_Type"),length(col("Item_Type"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE

# COMMAND ----------

df = df.withColumn("current date", current_date())
df = df.withColumn("after week", date_add(col("current date"), 7))
df.display()

# COMMAND ----------

df = df.withColumn("dates_diff",date_diff(col("after week"), col("current date")))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dealing with nulls
# MAGIC

# COMMAND ----------

# drop null values
df = df.dropna()


# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({
    "Item_Weight":0.0,
    "Outlet_Size":"Unknown"
})

df.display()

# COMMAND ----------

average = df.select(avg(col("Item_Weight"))).first()[0]
average

# COMMAND ----------

df = df.fillna({
    "Item_Weight": average
})

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split & indexing

# COMMAND ----------

df.withColumn("Outlet_Type",split(col("Outlet_Type")," ")).display()

# COMMAND ----------

df.withColumn("Outlet_Type",split(col("Outlet_Type")," ")[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE

# COMMAND ----------


df2 = df.withColumn("Outlet_Type", split(col("Outlet_Type"), " "))
df2.withColumn("Outlet_Type",explode(col("Outlet_Type"))).display()