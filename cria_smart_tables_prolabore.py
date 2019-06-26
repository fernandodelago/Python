#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''#retirar quando for implantar'''
import findspark
findspark.init()
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


# In[2]:


'''Bibliotecas utilizadas no processo'''
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SQLContext, HiveContext, SparkContext, SparkConf
import os
import sys
import argparse
import logging
import pandas as pd


# In[3]:


'''Utilizada para validação de datas entre tabelas'''
from datetime import date, datetime
from valida_data_ref_carga import verifica_data_ref_carga


# ## Definições para o contexto do Spark

# In[4]:


conf = SparkConf().setAppName("Test_movto_validos").setMaster("local[2]")


# In[5]:


spark= SparkSession.builder.enableHiveSupport().config(conf=conf).getOrCreate()


# In[6]:


spark_hive = HiveContext.getOrCreate(spark)


# In[7]:


#retirar quando for implantar
#if sc.startTime != 0:    sc.stop()


# In[8]:


sc = SparkContext.getOrCreate()


# In[9]:


sql_ctx = SQLContext(sc)


# In[10]:


hive_database = 'vc'


# ## Definições de mensagem de erro

# In[11]:


'''Test - Retirar na implantacao
o arquivo mylog.log vai ficar no mesmo diretório do notebook e/ou programa .py'''
fhandler = logging.FileHandler(filename='mylog.log', mode='a')


# In[12]:


logger = logging.getLogger("")


# In[13]:


logging.basicConfig(level=logging.INFO, format='%(asctime)s: SMART_TABLES %(message)s')
#formatter = logging.Formatter('%(asctime)s: SMART_TABLES %(message)s')
#logger.setLevel(logging.INFO)
#fhandler.setFormatter(formatter)


# In[14]:


logger.addHandler(fhandler)


# In[15]:


def setError(msg):
    '''Função que grava informações de erro no log/YARN
    Parâmetros de Entrada:
    msg - String - Texto da mensagem a gravar na log'''
    logger.error(msg)
    sys.exit(1)


# In[16]:


def setInfo(msg):
    '''Função que grava informações de informação do processamento no log/YARN
    Parâmetros de Entrada:
    msg - String - Texto da mensagem a gravar na log'''
    logger.info(msg)


# # Recebendo Parâmetros de Entrada

# In[32]:


smart_table_name = sys.argv[1]


# # Funções principais

# In[18]:


'''Formatação/recepção de variáveis para início do processamento'''
smart_table_name = 'movto_valido'
setInfo("## Parametro recebido: {}".format(smart_table_name))


# In[19]:


def movto_valido(hive_database):
    ''' ## Movimentos Validos ##
        Essa base é gerada através da tabela vcdt254.
        A tabela se encontra na database "vc"
        Parâmetros de Entrada:
        hive_database - nome do database no hive onde está/estão a(s) tabela(s)'''
    
    setInfo("## Fase 01 - Seleciona publico fora do range 700.000 e 799.999 ##")
    
    schema_fase01 = StructType([
        StructField("nr_prop",         StringType(),True),
        StructField("nr_parc_prop",    StringType(),True),
        StructField("nr_apol",         StringType(),True),
        StructField("nr_cerf_segr",    StringType(),True),
        StructField("cd_segd",         StringType(),True),
        StructField("nr_sequ_movi",    StringType(),True),
        StructField("cd_prod_segd",    StringType(),True),
        StructField("cd_subp_segd",    StringType(),True),
        StructField("dt_oper_cntb",    StringType(),True),
        StructField("dt_gera_cntb",    StringType(),True),
        StructField("cd_even_cntb",    StringType(),True),
        StructField("cd_idef_oper",    StringType(),True),
        StructField("nr_cnta_cntb",    StringType(),True),
        StructField("tp_cntb",         StringType(),True),
        StructField("vl_lanc_cntb",    DecimalType(17,2),True),
        StructField("tx_info_extt_ha", StringType(),True),
        StructField("dh_ulti_atlz",    TimestampType(),True),
        StructField("dat_ref_carga",   StringType(),True)
    ])
    
    #Busca a última data de carga da tabela de movimentos válidos
    dt_max_tabela = spark_hive.sql("""select max(`dat_ref_carga`) as dt_max 
                                      from {0}.vcdt254""".format(hive_database)).collect()
    
    dt_base_carga = datetime.strptime(dt_max_tabela[0].dt_max,'%Y-%m-%d')
    
    df_fase01 = spark_hive.createDataFrame(sc.emptyRDD(),schema=schema_fase01)
    
    df_fase01 = spark_hive.sql("""select  
                                     nr_prop
                                    ,nr_parc_prop
                                    ,nr_apol
                                    ,nr_cerf_segr
                                    ,cd_segd
                                    ,nr_sequ_movi  
                                    ,cd_prod_segd  
                                    ,cd_subp_segd  
                                    ,dt_oper_cntb  
                                    ,dt_gera_cntb  
                                    ,cd_even_cntb  
                                    ,cd_idef_oper  
                                    ,nr_cnta_cntb  
                                    ,tp_cntb
                                    ,vl_lanc_cntb
                                    ,tx_info_extt_ha
                                    ,dh_ulti_atlz
                                    ,dat_ref_carga
                                  from {db}.vcdt254
                                  where cast(nr_cnta_cntb as int) not between 700000 and 799999
                                    and dat_ref_carga = {dt_base_carga}""".format(db=hive_database,
                                                                                  dt_base_carga=dt_base_carga.date()))
    df_fase01.cache()
    
    setInfo("## Fase 02 - Identifica contas com soma total de saldo igual a zero ##")
    
    schema_fase02 = StructType([
        StructField("nr_cnta_cntb",StringType(),True),
        StructField("total_lanctos",DecimalType(17,2),True)
    ])
    
    df_fase01.createOrReplaceTempView("tb_vc254_fase01")
    
    df_fase02 = spark_hive.createDataFrame(sc.emptyRDD(),schema=schema_fase02)
    
    df_fase02 = spark_hive.sql("""select nr_cnta_cntb
                                          ,nr_prop
                                          ,nr_apol
                                          ,nr_cerf_segr
                                          ,nr_parc_prop
                                          ,cd_segd  
                                     ,sum(vl_lanc_cntb) as total_lanctos 
                                  from tb_vc254_fase01
                                  group by nr_cnta_cntb
                                          ,nr_prop
                                          ,nr_apol
                                          ,nr_cerf_segr
                                          ,nr_parc_prop
                                          ,cd_segd                                        
                                  having total_lanctos = 0""")
    df_fase02.cache()

    setInfo("## Fase 03 - Retira do publico da Fase 01 o publico encontrado na Fase 02 ##")
    
    df_fase02.createOrReplaceTempView("tb_vc254_fase02")
    
    df_fase03 = spark_hive.createDataFrame(sc.emptyRDD(),schema=schema_fase01)
    
    df_fase03 = spark_hive.sql("""select  
                                 f1.nr_prop
                                ,f1.nr_parc_prop
                                ,f1.nr_apol
                                ,f1.nr_cerf_segr
                                ,f1.cd_segd
                                ,f1.nr_sequ_movi  
                                ,f1.cd_prod_segd  
                                ,f1.cd_subp_segd  
                                ,f1.dt_oper_cntb  
                                ,f1.dt_gera_cntb  
                                ,f1.cd_even_cntb  
                                ,f1.cd_idef_oper  
                                ,f1.nr_cnta_cntb  
                                ,f1.tp_cntb
                                ,f1.vl_lanc_cntb
                                ,f1.tx_info_extt_ha
                                ,f1.dh_ulti_atlz
                                ,f1.dat_ref_carga
                            from tb_vc254_fase01 f1
                            left join tb_vc254_fase02 f2
                              on f1.nr_prop      = f2.nr_prop
                             and f1.nr_apol      = f2.nr_apol
                             and f1.nr_cerf_segr = f2.nr_cerf_segr
                             and f1.nr_parc_prop = f2.nr_parc_prop
                             and f1.cd_segd      = f2.cd_segd
                            where f2.nr_prop      is null
                              and f2.nr_apol      is null
                              and f2.nr_cerf_segr is null
                              and f2.nr_parc_prop is null
                              and f2.cd_segd      is null""")
    
    df_fase03.cache()
    
    setInfo("## Fase 04 - Formata smart table analitica de movimentos validos com data de geracao")
    
    df_fase03.write.mode("overwrite").saveAsTable("pcp.vc_movtos_validos_analitico")
    
    setInfo('## Fim do Processo - Movto_validos')


# In[20]:


def comissoes(spark_hive, datetime, hive_database):
    '''Inicia o processo de criação da smart table comissoes.
    Parâmetros de Entrada:
    spark_hive    - contexto do hive para acesso as tabelas
    datetime      - biblioteca com funções de data e hora
    hive_database - nome da database no Hive'''
    
    setInfo("##Entrou em comissoes")
    
    setInfo("## Fase 00 - Valida data das cargas das bases")
    
    lista_tabelas = ['vcdt254','vcdt076','vcdt107','vcdt321']
    #lista_tabelas = ['vc_movtos_validos_analitico','vcdt076','vcdt107','vcdt321']
    setInfo(str(lista_tabelas))
    
    db_name = [hive_database,hive_database,hive_database,hive_database]
    setInfo("## db entrada => " + str(db_name))
    
    #chamada para verificar se todas as tabelas estão com a mesma data de carga
    status_tables_list, data_base_carga = verifica_data_ref_carga(spark_hive,datetime,lista_tabelas,db_name)

    for idx, var in enumerate(status_tables_list):
        setInfo("## Chave: " + str(idx) + " - Valor: " + str(var))
    
    headers = ['status','tabela','descricao','sequencia']
    seq_linhas = [1,2,3,4]
    
    status_tables_list.append(seq_linhas)
    
    df_pd = pd.DataFrame.from_dict(dict(zip(headers,status_tables_list)))
    
    schema_status_tables = StructType([
        StructField("status",         StringType(),True),
        StructField("tabela",    StringType(),True),
        StructField("descricao",         StringType(),True),
        StructField("sequencia",         IntegerType(),True)
    ])
    
    spark_df = spark_hive.createDataFrame(df_pd,schema=schema_status_tables)
    
    spark_df.registerTempTable("tb_status_table")
    
    if 'NOK' in status_tables_list[0]:
                
        setInfo("## Bases Origem com datas de carga diferente!!! ##")
        setInfo("## Data base de carga: {} ##".format(data_base_carga))
        setInfo("")
        
        spark_hive.sql("""insert into pcp.vc_status_carga_tables
                          select
                              current_timestamp()
                             ,tabela
                             ,sequencia
                             ,status
                             ,descricao
                          from tb_status_table""")
        
        setInfo("##Saiu de comissoes")
        sys.exit(1)
    
    setInfo("##Bases com datas de carga batidas! Continuando")
    setInfo("##Saiu de comissoes")


# In[21]:


def opcao_03():
    '''Expansão futura - Novas smart tables'''
    
    setInfo("##Entrou na opcao 03")


# ## Aqui sera a funcao principal

# In[28]:


'''Função principal'''

movto_valido(hive_database)

if smart_table_name == "comissoes":
    comissoes(spark_hive, datetime,hive_database)
elif smart_table_name == "opcao_03":
    opcao_03()
else:
    setInfo("## Smart Table Invalida ou nao informada. ##")
    
setInfo("### FIM DO PROCESSO ###")


# In[29]:


spark_hive.sql("""select * from pcp.vc_status_carga_tables order by data_processamento desc, fase""").show(truncate=False)


# In[26]:


spark_hive.sql("""truncate table pcp.vc_status_carga_tables""")

