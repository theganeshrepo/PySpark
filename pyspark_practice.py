#!/usr/bin/env python
# coding: utf-8

# In[1]:


# pip install pyspark


# In[2]:


# pip install pyspark findspark


# In[3]:


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import count
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[4]:


spark = SparkSession.builder\
        .appName("Day1_Spark_Basics")\
        .getOrCreate()


# In[5]:


people_schema = StructType([
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("City",StringType(),True)
])


# In[6]:


df = spark.read.csv("people.csv", header=True, inferSchema=False, schema=people_schema)


# In[7]:


df.show()


# In[8]:


df.groupBy("City") \
  .agg(count("*").alias("total_people")) \
  .show(truncate=False)


# In[9]:


city_wise_count = df.groupBy("City")\
    .count().show(truncate=False)


# In[10]:


df.select(df.Name,df.Age).show()


# In[11]:


filtered_df = df.filter(df.Age>=30)#.select(df.Name,df.Age).show()
filtered_df.select(col("Name"),col("Age")).show()


# #### Add a new column called age_group based on the following logic:
# #### "Young" if age < 30
# #### "Adult" if age is between 30 and 40 (inclusive)
# #### "Senior" if age > 40

# In[12]:


adding_age_group = df.withColumn("age_group",when(df.Age<30,"Young")
                                 .when((df.Age>=30) & (df.Age <=40),"Adult")
                                 .when(df.Age>40,"Senior"))
adding_age_group.show()


# #### Find the average age of people per city.
# #### Show the output as:
# #### city | avg_age

# In[13]:


CitywiseAverageAge = df.groupBy("City")\
    .agg(avg("Age").alias("CitywiseAverageAge"))


# In[14]:


CitywiseAverageAge.show()


# In[15]:


df.show()


# #### Remove all rows where the Age is null or missing

# In[18]:


remove_null = df.na.drop()


# In[19]:


remove_null.show()


# In[25]:


df.sort(df.Age.desc()).show()


# In[30]:


df.select("Name","Age","City")\
    .orderBy(df.Age.desc())\
    .limit(3)\
    .show()


# #### Read both CSVs into separate DataFrames
# 
# #### Combine them into one DataFrame
# 
# #### Remove any duplicate rows
# 
# #### Show the result

# In[41]:


mumbai_people_schema = StructType([
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("City",StringType(),True)
])


# In[42]:


mumbai_people_df = spark.read.csv("people_mumbai.csv",header=True, inferSchema=False, schema=mumbai_people_schema)


# In[43]:


pune_scheam = StructType([
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("City",StringType(),True)
])


# In[44]:


pune_people_df = spark.read.csv("people_pune.csv",header=True, inferSchema=False, schema=pune_scheam)


# In[46]:


pune_people_df.show()


# In[50]:


union_two_df = mumbai_people_df.union(pune_people_df).distinct()


# In[51]:


union_two_df.show()


# In[ ]:




