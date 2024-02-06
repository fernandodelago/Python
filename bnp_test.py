import pyspark.sql.functions as F 

df_clients_joined = df_account.alias('acct').join(df_customer.alias('cli'), on='customerid', how='inner') \
    .select('acct.customerid'
           ,'cli.forename'
           ,'cli.surname'
           ,'acct.accountid'
           ,'acct.balance'
    ).orderBy('customerid')

df_client_grouped = df_clients_joined.groupBy("customerid", "forename", "surname") \
    .agg(F.collect_list("accountid").alias("number_accounts"), \
         F.avg("balance").alias("avg_balance"), \
         F.sum("balance").alias("totalbalance"))

df_client_grouped = df_client_grouped.withColumn("qtd_acct", F.size("number_accounts")) 

df_client_grouped = df_client_grouped.select('forename', 
                                             'surname', 
                                             F.col('number_accounts').alias('accounts'), 
                                             F.col('qtd_acct').alias('numberAccounts'), 
                                             F.col('totalbalance').alias('totalBalance'), 
                                             F.col('avg_balance').alias('averageBalance'))