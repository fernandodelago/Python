import pyspark.sql.functions as F 

# I created this using Google Colab

# function to apply standard name columns
def udf_apply_pattern(df):
  df_2 = df
  for campo in df.columns:
    novo_campo = campo.lower().replace(' ','_')
    df_2 = df_2.withColumnRenamed(campo, novo_campo)
  return df_2

# read files
df_account = spark.read.csv('account_data.csv',header='True')
df_customer = spark.read.csv('customer_data.csv',header='True')

# standarlize the column names
df_account_std = udf_apply_pattern(df_account)
df_customer_std = udf_apply_pattern(df_customer)

# join the dataframes
df_clients_joined = df_account.alias('acct').join(df_customer.alias('cli'), on='customerid', how='inner') \
    .select('acct.customerid'
           ,'cli.forename'
           ,'cli.surname'
           ,'acct.accountid'
           ,'acct.balance'
    ).orderBy('customerid')

# grouping the data and generate some KPI
df_client_grouped = df_clients_joined.groupBy("customerid", "forename", "surname") \
    .agg(F.collect_list("accountid").alias("number_accounts"), \
         F.avg("balance").alias("avg_balance"), \
         F.sum("balance").alias("totalbalance"))

# generate another KPI
df_client_grouped = df_client_grouped.withColumn("qtd_acct", F.size("number_accounts")) 

# rename the columns
df_client_grouped = df_client_grouped.select('forename', 
                                             'surname', 
                                             F.col('number_accounts').alias('accounts'), 
                                             F.col('qtd_acct').alias('numberAccounts'), 
                                             F.col('totalbalance').alias('totalBalance'), 
                                             F.col('avg_balance').alias('averageBalance'))
