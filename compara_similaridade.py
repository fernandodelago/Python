#!/usr/bin/env python
# coding: utf-8

def string_mais_similar_fuzzy(list_target,list_compare):
  '''
  Faz comparacao por similaridade.
  Entrada:
    list_target - Formato de lista - lista base para comparacao
    list_comparare - Formato de lista - lista de valores para comparar com a lista base
  Saida:
    df_result - Formato dataframe Spark - resultado da comparacao, sendo
      str_list_target - valor da list_target
      str_list_compare - valor da list_compare
      score - percentual de similaridade
      
  Exemplo de chamada:
  lista_target = spark.sql('select * from tpv_base').rdd.flatMap(lambda x: x).collect()
  lista_a_comparar = spark.sql('select * from tpv_valores').rdd.flatMap(lambda x: x).collect()
   df_similar_fuzzy = string_mais_similar_fuzzy(lista_target, lista_a_comparar)
  '''
  from fuzzywuzzy import fuzz
  from pyspark.sql import functions as F
  list_scores = []
  for s1 in list_target:
    for s2 in list_compare:
      Ratio = fuzz.ratio(s1,s2)
      list_scores.append([s1,s2,Ratio])
  df = spark.createDataFrame(sc.parallelize(list_scores), ["str_list_target", "str_list_compare","score"])
  df.createOrReplaceTempView("fuzzy_results")
  df_result = spark.sql("""
  select a.str_list_target
        ,b.str_list_compare
        ,a.max_score as score from
  (select str_list_target
         ,max(score) as max_score 
   from fuzzy_results 
   group by str_list_target) a
  inner join fuzzy_results b on (a.str_list_target = b.str_list_target and a.max_score = b.score)
  """)
  return(df_result)