É necessário setar este parâmetro na configuração do Spark.

spark.sql("set spark.sql.sources.partitionOverwriteMode = dynamic")

# Excluir a tabela que usaremos de testes
spark.sql('drop table if exists default.tb_teste_particao')

df_p01=spark.createDataFrame([(1,"Fernando","2020-03-01"),(2,"Gabrielle","2020-04-01")],["id","nome","dt_ref"])
print(df_p01.show())

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  1| Fernando|2020-03-01|
|  2|Gabrielle|2020-04-01|
+---+---------+----------+

fl_exists = spark.sql('''show tables in default like "tb_teste_particao"''').count()

if fl_exists == 0:
    # Cria a tabela se não existe
    df_p01.write.mode("overwrite").partitionBy("dt_ref").format("orc").saveAsTable("default.tb_teste_particao")
    print('criou')
else:
    # carrega a tabela, pois já existe
    df_p01.write.mode("overwrite").format("orc").insertInto("default.tb_teste_particao",overwrite=True)
    print('carregou')

# Verificando resultado
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

criou
+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
+-----------------+

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  1| Fernando|2020-03-01|
|  2|Gabrielle|2020-04-01|
+---+---------+----------+

df_p01_new = spark.createDataFrame([(99,"Jose","2020-03-01")],["id","nome","dt_ref"]) 
df_p01_new.show()

+---+----+----------+
| id|nome|    dt_ref|
+---+----+----------+
| 99|Jose|2020-03-01|
+---+----+----------+

spark.sql('select * from default.tb_teste_particao').show()

### MESMO COM O PARAMETRO .partitionBy("dt_ref") A TABELA É SOBREESCRITA INTEIRA
print('Antes')
spark.sql('select * from default.tb_teste_particao').show()
df_p01_new.write.partitionBy("dt_ref").mode("overwrite").format("orc").saveAsTable("default.tb_teste_particao")

# Verificando resultado
print('Depois')
spark.sql('select * from default.tb_teste_particao').show()

Antes
+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  2|Gabrielle|2020-04-01|
|  1| Fernando|2020-03-01|
+---+---------+----------+

Depois
+---+----+----------+
| id|nome|    dt_ref|
+---+----+----------+
| 99|Jose|2020-03-01|
+---+----+----------+

### Recriando o exemplo
spark.sql('drop table default.tb_teste_particao')
df_p01.write.mode("overwrite").partitionBy("dt_ref").format("orc").saveAsTable("default.tb_teste_particao")
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao').show()

+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
+-----------------+

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  2|Gabrielle|2020-04-01|
|  1| Fernando|2020-03-01|
+---+---------+----------+

df_p02=spark.createDataFrame([(3,"Flavia","2020-03-01"),(4,"Lucas","2020-04-01")],["id","nome","dt_ref"])
df_p02.show()

+---+------+----------+
| id|  nome|    dt_ref|
+---+------+----------+
|  3|Flavia|2020-03-01|
|  4| Lucas|2020-04-01|
+---+------+----------+

### USANDO partitionBy COM OPÇÃO append (insere novas linhas e mantem as existentes)
df_p02.write.mode("append").partitionBy("dt_ref").format("orc").saveAsTable("default.tb_teste_particao")

# Verificando resultado
print('Depois')
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

Depois
+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
+-----------------+

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  1| Fernando|2020-03-01|
|  3|   Flavia|2020-03-01|
|  2|Gabrielle|2020-04-01|
|  4|    Lucas|2020-04-01|
+---+---------+----------+

df_p03=spark.createDataFrame([(5,"Toddinho","2020-08-01"),(6,"Elsa","2020-09-01")],["id","nome","dt_ref"])
df_p03.show()

+---+--------+----------+
| id|    nome|    dt_ref|
+---+--------+----------+
|  5|Toddinho|2020-08-01|
|  6|    Elsa|2020-09-01|
+---+--------+----------+

print('Antes')
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

### INCLUINDO NOVO REGISTRO USANDO COM MODO overwrite EM NOVA PARTIÇÃO
df_p03.write.mode("overwrite").insertInto("default.tb_teste_particao", overwrite=True)

# Verificando resultado
print('depois')
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

Antes
+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  1| Fernando|2020-03-01|
|  3|   Flavia|2020-03-01|
|  2|Gabrielle|2020-04-01|
|  4|    Lucas|2020-04-01|
+---+---------+----------+

depois
+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
|dt_ref=2020-08-01|
|dt_ref=2020-09-01|
+-----------------+

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  3|   Flavia|2020-03-01|
|  1| Fernando|2020-03-01|
|  4|    Lucas|2020-04-01|
|  2|Gabrielle|2020-04-01|
|  5| Toddinho|2020-08-01|
|  6|     Elsa|2020-09-01|
+---+---------+----------+

df_p04=spark.createDataFrame([(7,"Matheus","2020-03-01"),(8,"Oiram","2020-11-01")],["id","nome","dt_ref"])
df_p04.show()

+---+-------+----------+
| id|   nome|    dt_ref|
+---+-------+----------+
|  7|Matheus|2020-03-01|
|  8|  Oiram|2020-11-01|
+---+-------+----------+

print('Antes')
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

### INCLUINDO NOVO REGISTRO  COM MODO overwrite EM NOVA PARTIÇÃO e MESMA PARTIÇÃO
df_p04.write.mode("overwrite").format("orc").insertInto("default.tb_teste_particao", overwrite=True)

# Verificando resultado
print('Depois')
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

Antes
+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  3|   Flavia|2020-03-01|
|  1| Fernando|2020-03-01|
|  4|    Lucas|2020-04-01|
|  2|Gabrielle|2020-04-01|
|  5| Toddinho|2020-08-01|
|  6|     Elsa|2020-09-01|
+---+---------+----------+

Depois
+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
|dt_ref=2020-08-01|
|dt_ref=2020-09-01|
|dt_ref=2020-11-01|
+-----------------+

+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  7|  Matheus|2020-03-01|
|  2|Gabrielle|2020-04-01|
|  4|    Lucas|2020-04-01|
|  5| Toddinho|2020-08-01|
|  6|     Elsa|2020-09-01|
|  8|    Oiram|2020-11-01|
+---+---------+----------+

df_p05=spark.createDataFrame([(10,"Rodrigo","2020-03-01"),(11,"Bianca","2020-04-01")],["dt_ref","id","nome"])
df_p05.show()

+------+-------+----------+
|dt_ref|     id|      nome|
+------+-------+----------+
|    10|Rodrigo|2020-03-01|
|    11| Bianca|2020-04-01|
+------+-------+----------+

print('Antes')
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

# SOBREESCREVENDO PARTICOES USANDO MODO overwrite SEM ALTERAR AS DEMAIS PARTICOES
df_p05.write.mode("overwrite").format("orc").insertInto("default.tb_teste_particao",overwrite=True)

# Verificando resultado
print('depois')
spark.sql('show partitions default.tb_teste_particao').show()
spark.sql('select * from default.tb_teste_particao order by dt_ref').show()

Antes
+---+---------+----------+
| id|     nome|    dt_ref|
+---+---------+----------+
|  7|  Matheus|2020-03-01|
|  2|Gabrielle|2020-04-01|
|  4|    Lucas|2020-04-01|
|  5| Toddinho|2020-08-01|
|  6|     Elsa|2020-09-01|
|  8|    Oiram|2020-11-01|
+---+---------+----------+

depois
+-----------------+
|        partition|
+-----------------+
|dt_ref=2020-03-01|
|dt_ref=2020-04-01|
|dt_ref=2020-08-01|
|dt_ref=2020-09-01|
|dt_ref=2020-11-01|
+-----------------+

+---+--------+----------+
| id|    nome|    dt_ref|
+---+--------+----------+
| 10| Rodrigo|2020-03-01|
| 11|  Bianca|2020-04-01|
|  5|Toddinho|2020-08-01|
|  6|    Elsa|2020-09-01|
|  8|   Oiram|2020-11-01|
+---+--------+----------+