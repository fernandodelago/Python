#!usr/bin/env python
#-*- coding:utf-8 -*-
'''
Detalhes para documentacao do teste
'''

# importacao de pacotes
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import SQLContext, SparkContext
import argparse
import sys

def consulta_table_shp_costcenter(hive, hive_database):
    """Consulta tabela shp_costcenter dentro do CDP"""
    print('#### Consulta tabela costcenter no CDP - INI ####')
    print(' ')

    schema_costcenter = StructType([
        StructField("costcenterjdecode",         StringType(), True),
        StructField("costcenterciacode",         StringType(), True),
        StructField("costcentername",            StringType(), True),
        StructField("costcentercountry",         StringType(), True),
        StructField("costcenterstate",           StringType(), True),
        StructField("costcenterreference",       StringType(), True),
        StructField("costcentersegment",         StringType(), True),
        StructField("costcenterfunction",        StringType(), True),
        StructField("costcentersubfunction",     StringType(), True),
        StructField("costcenterarea",            StringType(), True),
        StructField("costcenterregion",          StringType(), True),
        StructField("costcentermacroregion",     StringType(), True),
        StructField("costcenterstructure",       StringType(), True),
        StructField("costcentersubsidiary",      StringType(), True),
        StructField("costcentermanagerdirector", StringType(), True),
        StructField("distrib_key",               StringType(), True),
        StructField("costcenterfunctiongroup",   StringType(), True),
        StructField("costcenterexpensetype",     StringType(), True),
        StructField("costcenterlegalentity",     StringType(), True),
        StructField("costcenterbalanceconsol",   StringType(), True),
        StructField("no_planta",                 StringType(), True),
        StructField("costcenterdirectindirect",  StringType(), True),
        StructField("costcenterwarehousingprop", StringType(), True)
    ])

    df_fact = hive.createDataFrame(sc.emptyRDD(), schema_costcenter)

    df_costcenter = hive.sql("""select 
                                    costcenterjdecode
                                   ,costcenterciacode         
                                   ,costcentername            
                                   ,costcentercountry         
                                   ,costcenterstate           
                                   ,costcenterreference       
                                   ,costcentersegment         
                                   ,costcenterfunction        
                                   ,costcentersubfunction     
                                   ,costcenterarea            
                                   ,costcenterregion          
                                   ,costcentermacroregion     
                                   ,costcenterstructure       
                                   ,costcentersubsidiary      
                                   ,costcentermanagerdirector 
                                   ,distrib_key               
                                   ,costcenterfunctiongroup   
                                   ,costcenterexpensetype     
                                   ,costcenterlegalentity     
                                   ,costcenterbalanceconsol   
                                   ,no_planta                 
                                   ,costcenterdirectindirect  
                                   ,costcenterwarehousingprop
                                from shp_costcenter    
    """.format(db=hive_database))

    df_costcenter.cache()

    df_costcenter = registerTempTable("df_costcenter")
    print(" ")
    print(df_costcenter.head())

    df_costcenter_filtro01 = sqlContext.sql('select distinct(costcenterarea) from df_costcenter')

    print('# Quantidade de registros unicos costcenterarea')
    print(df_costcenter_filtro01.count())
    print(' ')

    print('# primeiros registros costcenterarea')
    print(df_costcenter_filtro01.head())

    print('#### Consulta tabela costcenter no CDP - INI ####')
    return df_costcenter_filtro01

def consulta_csv_accounts(sqlContext, data_raw):
    '''

    :param schema_account: layout do arquivo accounts.csv
    :param data_raw: dataframe com os dados do arquivo accounts.csv
    :param error_list: erros encontrados no processo de validacao
    :return: df_accounts_filtro01 - dataframe com dados de accounts filtrado
    '''

    print("#### CONSULTA CSV ACCOUNTS - INI ####")

    data_raw.createOrReplaceTempView('accounts_full')

    df_accounts_filtro01 = sqlContext.sql('select distinct(accountmodel) from accounts_full')
    print(df_accounts_filtro01.count())
    print(df_accounts_filtro01.show())

    print("#### CONSULTA CSV ACCOUNTS - FIM ####")
    return df_accounts_filtro01

def acessa_cdp(hive):
    #Funçao que verifica a conexao com o CDP.

    print('#### Faz conexao com o CDP e chama funcao de consulta a CostCenter - INI ####')

    #Mensagens
    msg_ok = 'Accounts - Validacao de Accounts com CostCenter sem problemas encontrados.'
    msg_nok = 'Accounts - Erro na verificacao da existencia da accountmodel na costcenterarea da CostCenter.'
    msg_nok_detail = 'Os accountnames nao encontrados na costcenter foram {}'

    parser = argparse.ArgumentParser()

    #parser.add_argument('--env', metavar='environment', required=True, dest='env', help='environment context')
    parser.add_argument('prd', metavar='environment', required=True, dest='env', help='environment context')

    args = parser.parse_args()
    hive_env = args.env

    print(' ')

    hive_database = "{env}_product_expense_cascsa".format(env=hive_env)
    
    print('#---- Hive Datadabse ----#')
    print(hive_database)

    hive.setConf("hive.exec.dynamic.partition", "true")
    hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    cont = 0

    consulta_table_shp_costcenter(hive, hive_database)
    #retorna df_costcenter_filtro01

    print('#### Faz conexao com o CDP e chama funcao de consulta a CostCenter - FIM ####')
    return df_costcenter_filtro01


def cria_table_erro(hive):

    print('#### Create da tabela de erro - INI ####')
    print(' ')
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', metavar='environment', required=True, dest='env',
                        help='environment context')
    args = parser.parse_args()
    hive_env = args.env

    hive_database = "{env}_product_expense_cascsa".format(env=hive_env)
    print('**********')
    print(hive_database)

    hive.setConf("hive.exec.dynamic.partition", "true")
    hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    df_table_error = hive.sql("""CREATE external TABLE if not exists {db}.warning_casc 
                       (   sharepoint_list    STRING    COMMENT 'Sharepoint_list'
                       ,   status             STRING    COMMENT 'Status'
                       ,   status_all         STRING    COMMENT 'Status All'
                       ,   validation_number  BIGINT    COMMENT 'Validation Number'
                       ,   validation_descrip STRING    COMMENT 'Validation Description'
                       ,   error_descr        STRING    COMMENT 'Error Description'
                       ,   try_validate       TIMESTAMP COMMENT 'Try Validate'
                       ,   last_validate      TIMESTAMP COMMENT 'Last Validate' ) 
                       STORED AS PARQUET LOCATION 'hdfs://nameservice1/prd/product/expense_cascsa/warning_casc'""".format(db=hive_database) )

    df_table_error.registerTempTable("df_table_error")

    print('#### Create da tabela de erro - FIM ####')
    
def dt_ult_validacao(hive):
    ''' Busca a data da ultima valicao OK para accounts'''
    
    print("#### CONSULTA ULTIMA DT VALIDACAO OK - INICIO ####")
    
    dt_ult_validacao = hive.sql("""select max(last_validate) from warning_casc
                                   where {db}.sharepoint_list = 'Accounts'
                                   and   status_all = 'OK'""")
    
    print("#### CONSULTA ULTIMA DT VALIDACAO OK - FIM ####")
    
    return dt_ult_validacao

def grava_tabela_erro(error_list, hive):

    '''Irá gravar os códigos que idetificam como finalizou o processamento de validaçao do arquivo accounts.'''

    print("#### GRAVA TABELA ERRO - INICIO ####")
    
    hive_database="{env}_product_expense_cascsa".format(env=hive_env)
    print('**********')
    print(hive_database)

    schema_table_erro = StructType([
    StructField("Status"           ,StringType(),True),
    StructField("fase_validacao"   ,StringType(),True),
    StructField("detalhe_validacao",StringType(),True)])
    
    print('#--- Detalhes da lista de erros ---#')
    error_list.createOrReplaceTempView('error_full')
	
    dt_ult_validacao = consulta_ultima_validacao()
    
    print("#--- Total Warning NOK ---#")
    print(total_error_nok.show())
    
    if 'NOK' in error_list[0]:
        var_status_all = 'NOK'
    else:
        var_status_all = 'OK'

    df_insert = hive.sql("""insert into {db}.warning_casc
	                        select
	                        	'Accounts'
                               ,{status}
                               ,{status_all}
                               ,'1'
                               ,{fase_validacao}
                               ,{detalhe_validacao}
                               ,current timestamp()
                               ,{dt_ult_validacao}
	                        from error_full""".format(status=error_list[0],
                                                      status_all=var_status_all,
                                                      fase_validacao=error_list[1],
                                                      detalhe_validacao=error_list[2],
                                                      dt_ult_validacao=dt_ult_validacao,db=hive_database))
    
    print(error_list)
    
    print("#### GRAVA TABELA ERRO - FIM ####")


def valida_duplicados(data_raw, sqlContext, error_list):

    '''
    Valida se existem dados duplicados nas colunas accountjdecode e accountmodel
    :param data_raw: dataset com os dados cru (originais)
    :param sqlContext: usado para comandos SQL
    :param sc: parâmetro do Spark
    :param error_list: Lista com os Erros encontrados
    :return: lista de erros
    '''

    print('#### Trata Duplicados - INI ####')

    Msg_OK  = 'Accounts - Nao ha informacao duplicada (AccountJDECode + AccountModel).'
    Msg_NOK = 'Accounts - Duplicidades Encontradas em Accounts.'
    Msg_Duplicidades_detail = 'As duplicidades para {}.'

    data_raw.createOrReplaceTempView('accounts_full')
    
    print('--------------------------')
    colunas_concat = sqlContext.sql('''select concat(accountjdecode, accountmodel) as JDECode_Model
                                       from accounts_full 
                                       group by concat(accountjdecode, accountmodel''')

    print('Qtd retornada : ' + colunas_concat.show())

    result_query_duplicados2 = sqlContext.sql('''select count(concat(accountjdecode, accountmodel)) as total 
                                                 from accounts_full 
                                                 group by concat(accountjdecode, accountmodel) 
                                                 having count(concat(accountjdecode, accountmodel)) > 1''')
    print('resultado : '.format(result_query_duplicados2.show()))
    print('--------------------------')

    qtd_duplicados = result_query_duplicados2.collect()
    print(qtd_duplicados[0][0])

    if qtd_duplicados[0][0] <= 1 or qtd_duplicados[0][0] == None:
        print('Nao ha duplicado')
        error_list[0].append('OK')
        error_list[1].append(Msg_OK)
        error_list[2].append('')
    else:
        print("Encontrada duplicidade")
        error_list[0].append('NOK')
        error_list[1].append(Msg_NOK)

        lista_duplicados = [[]]

        for cont, var in enumerate(colunas_concat.collect()):
            lista_duplicados[0].append(var[0])

        error_list[2].append(Msg_Duplicidades_detail.format(lista_duplicados))
        print(error_list)

    print('#### Trata Duplicados - FIM ####')
    return error_list

def valida_campos_obrigatorios(data_raw,sqlContext, error_list):
    '''
    Validaçao Tecnica (Básica):
    VT01 - Os campos obrigatórios quando nao informados, acarretará no cancelamento do processamento e por consequên-
    cia, nao será efetuada a carga dos dados.
    Campos Obrigatorios: accountjdecode, accountmodel
    '''

    print("#### VALIDA_CAMPOS_OBRIGATORIOS - INI ####")

    #Mensagens de Erro possíveis
    Msg_OK  = 'Accounts - Sem erros de campos nulos.'
    Msg_NOK = 'Accounts - Erro de campo com valor nulo.'
    Msg_JDECode_Nulo = 'Accounts - Quantidade de AccountsJDECode nulos encontrados foram {}'
    Msg_Model_Nulo = 'Accounts - Quantidade de AccountModel nulos encontrados {}'

    data_raw.createOrReplaceTempView('accounts_full')
    print('Count no AccountJDECODE')
    var_qtd_null_jdecode = sqlContext.sql('''select count(*) as val_nulos 
                                             from accounts_full 
                                             where accountjdecode is null ''')

    print(var_qtd_null_jdecode.show())

    qtd_null_jdecode = var_qtd_null_jdecode.collect()

    if qtd_null_jdecode[0][0] > 0:
        print('Achamos um nulo no JDECode!!!')
        print(' ')
        print(error_list)
        error_list[0].append('NOK')
        error_list[1].append(Msg_NOK)
        error_list[2].append(Msg_JDECode_Nulo.format(qtd_null_jdecode[0][0]))
    else:
        print('Nao ha valores nulos em JDECode.')
        print(' ')
        error_list[0].append('OK')
        error_list[1].append(Msg_OK)
        error_list[2].append('')

    print(' ')
    print('Count no AccountMODEL')
    var_qtd_null_model = sqlContext.sql('''select count(*) 
                                           from accounts_full 
                                           where accountmodel is null ''')

    print(var_qtd_null_model.show())
    
    qtd_null_model = var_qtd_null_model.collect()
    
    if qtd_null_model[0][0] > 0:
        print('Achamos um nulo no Model!!!')
        print(' ')
        print(error_list)
        error_list[0].append('NOK')
        error_list[1].append(Msg_NOK)
        error_list[2].append(Msg_Model_Nulo.format(qtd_null_model[0][0]))
    else:
        print('Nao ha valores nulos em Model.')
        print(' ')
        error_list[0].append('OK')
        error_list[1].append(Msg_OK)
        error_list[2].append('')

    print("#### VALIDA_CAMPOS_OBRIGATORIOS - FIM ####")
    return error_list


def load_accounts_csv(sqlContext):

    print('##### LOAD_ACCOUNTS_CSV - INI #####')
    schema_account = StructType([
        StructField("accountjdecode", StringType(), True),
        StructField("accountname", StringType(), True),
        StructField("accountciacode", StringType(), True),
        StructField("accounttype", StringType(), True),
        StructField("accounttypegroup", StringType(), True),
        StructField("accountsub", StringType(), True),
        StructField("accountfr", StringType(), True),
        StructField("accountfrptype", StringType(), True),
        StructField("accountfrptypesub", StringType(), True),
        StructField("accountgroup", StringType(), True),
        StructField("accountmodel", StringType(), True),
        StructField("accounttranslation", StringType(), True),
        StructField("frpline", StringType(), True)
    ])

    # file:///efs/home/ps073127/prd/cascsa_af/framework_sharepoint/accounts/csv/accounts.csv
    # file:///efx/home/ps073127/prd/cascsa_af/framework_validacao/csv_test/accounts1.csv

    
    data_raw = sqlContext.read.load('file:///home/delago/accounts1.csv',
                                    format='com.databricks.spark.csv',
                                    header=False,
                                    delimiter="|",
                                    schema=schema_account)


    data_raw.cache()  # armazena em cache para processar na memoria RAM

    data_raw.createOrReplaceTempView('table1')

    df2 = sqlContext.sql("SELECT * from table1")
    print(df2.show())  # resultado da query

    print('##### LOAD_ACCOUNTS_CSV - FIM #####')
    return df2, schema_account


def main_csv(error_list):

    '''
    Leitura do accounts.csv
    '''

    msg_ok  = 'Accounts - Todos os AccountNames existem na CostCenter.'
    msg_nok = 'Accounts - Existem AccountNames sem relacionamento com CostCenter'
    msg_nok_detail = 'Accounts - Os AccountNames nao encontrados foram {}.'

    print("#### MAIN_CSV - Leitura do accounts.csv - INI ###")
    print(" ")
    sc = SparkContext("local", "valida_accounts")
    sqlContext = SQLContext(sc)
    hive = HiveContext(sc)
    hive.setConf("hive.exec.dynamic.partition", "true")
    hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    #rdd1 = sc.textFile('/efs/home/ps073127/prd/cascsa_af/framework_validacao/csv_test/accounts1.csv')
    #rdd1 = sc.textFile('/home/delago/accounts1.csv')
    #rdd1 = sc.textFile('/efs/home/ps073127/prd/cascsa_af/framework_sharepoint/accounts/csv/accounts.csv')

    '''
    Regra de Negócios (RN): 
    RN01 - A informaçao do campo accountmodel tem que existir dentro da tabela costcenter que está no CDP. 
    A validaçao deste relacionamento será pelo campo CostCenterArea.
      |--> Observaçao: Estamos assumindo que a tabela de CostCenter já foi validada e carregada antes deste processo.
      |--> Tratamento de Exceçao: Quando nao for encontrada informaçao (do campo accountmodel) no arquivo costcenter,
       o processamento deve ser interrompido e a carga nao deve ser efetuada. 
      
    '''
    #Recebe retorno do load_accounts_csv
    data_raw, schema_accounts = load_accounts_csv(sqlContext)

    print(" ")
    print("### Quantidade de Linhas ###")
    df3 = data_raw.count()
    print(df3)

    # chama tratamento de campos obrigatorios
    error_list = valida_campos_obrigatorios(data_raw, sqlContext, error_list)

    # chama tratamento de campos duplicados
    error_list = valida_duplicados(data_raw, sqlContext, error_list)

    # chama checagem e conexao com o CDP
    df_costcenter_filtro01 = acessa_cdp(hive)

    df_distinct_cc = []
    df_distinct_cc = df_costcenter_filtro01

    print("PT01")

    # consulta dados do arquivo accounts.csv
    df_accounts_filtro01 = consulta_csv_accounts(sqlContext, data_raw)

    df_distinct_acct = []
    df_distinct_acct = df_accounts_filtro01
    print(df_accounts_filtro01)

    lista_not_in_cc = []
    flag_erro = 0

    for var in enumerate(df_distinct_acct.collect()):
        if var not in df_distinct_cc:
            lista_not_in_cc.append(var)
            flag_erro = 1
        else:
            pass

    if flag_erro == 1:
        error_list[0].append('NOK')
        error_list[1].append(msg_nok)
        error_list[2].append(msg_nok_detail.format(lista_not_in_cc))

    cria_table_erro(sc)

    #grava_erro
    grava_tabela_erro(error_list, hive)

    print("#### MAIN_CSV - Leitura do accounts.csv - FIM ###")
    return error_list

def main_principal(error_list, arg):
    '''Rotina Principal'''

    print("Rotina Principal")
    print(" ")

    if arg == 'csv':
        flag_erro = main_csv(error_list)
        print(error_list)
        if flag_erro == 1:
            sys.exit(1)
    else:
        sys.exit(1)

    print(' ')
    print('### FIM DO PROCESSAMENTO ###')

if __name__ == "__main__":
    print('######### INICIO DO PROGRAMA ##########')
    print(" ")
    print("Inicio processo validacao accounts")
    print(" ")
    # lista tripla com dados a serem preenchidas com os possíveis erros
    # error_list[0] = flag de falha /0 sem falha/ 1 com falha/ tamanho da lista dependerá da quantidade de validacoes
    # error_list[1] = escrever aqui o tipo de erro - a string conforme sprint4 / Caso nao tenha deixar no error
    # error_list[2] = disponivel futuro
    error_list = [[], [], []]
    parser = argparse.ArgumentParser()

    # comando que vira externo se validara csv ou hive
    #parser.add_argument("file")
    #args = parser.parse_args()

    #------------------------------------------#
    #-- Arquivo NOK - campo JDECode nulo
    #nome_arquivo='accounts.csv'

    #-- Arquivo OK para todas as validações
    #nome_arquivo='accounts1.csv'
    temp_var = 'csv'

    #-- Arquivo NOK - campo AccounttJDECode||AccountModel duplicado
    #nome_arquivo='accounts2.csv'

    print("Valores de error_list")
    print(error_list)
    print(" ")

    #main(error_list, args.file)
    main_principal(error_list, temp_var)
    print('#########    FIM DO PROGRAMA ##########')
