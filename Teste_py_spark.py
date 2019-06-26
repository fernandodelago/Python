#!/usr/bin/env python
# coding: utf-8

# ## Teste de Pyspark

# In[4]:


from pyspark.sql.types import *
from pyspark import SQLContext, HiveContext, SparkContext


# In[9]:


sc = SparkContext()


# In[10]:


sqlcontext = SQLContext(sc)


# In[11]:


hive_context = HiveContext(sc)


# In[12]:


rdd = sc.parallelize(range(1000))


# In[13]:


rdd.takeSample(False,5)


# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder.getOrCreate()


# In[6]:


df = spark.sql('''select 'spark' as hello ''')


# In[7]:


df.show()


# In[8]:


spark.stop()

