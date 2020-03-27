# Ingestão de arquivos CSV

# Controle de Versões
'''
|---------+-----------------+------------+-----------------------|
| Version | Author          | Date       | Description           |
|---------+-----------------+------------+-----------------------|
| 1.0     | Fernando Delago | 28/01/2020 | Código original       |
|---------+-----------------+------------+-----------------------|

# Arquivos que podem ser processados

* `insumos_cadastral.txt` - Arquivo com dados cadastrais de clientes e indicador de tipo de cartão.
* `insumos_faturas.txt` - Arquivo que possui dados de faturas de cartões de crédito.
* `insumos_pgto_fatura.txt` - Arquivo que possui dados de pagamentos de faturas de cartões de crédito.
* `insumos_seguros.txt` - Arquivo que possui dados de seguros contratados.
* `insumos_banca.txt` - ????
* `insumos_rf.txt` - Arquivo que possui dados de Roubo e Furto.
* `insumos_ge.txt` - ????
* `insumos_contr_ep.txt` - Arquivo com dados de contratação de empréstimos pessoais.

'''

import sys
import warnings
import pandas as pd
import time
import datetime
from datetime import datetime as dt
from dateutil import tz
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from IPython.display import HTML

print("#--- Bibliotecas Importadas ---#")

spark = SparkSession.builder.appName('Test_app').enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("spark.sql.execution.arrow.enabled","false").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc,spark)

print('#--- Spark Executado ---#')

- MANTER NOMES DAS TABELAS EM MAIUSCULO
- INFORMAR QUEM VAI UTILIZAR O SCRIPT
- O ARQUIVO CSV DEVE OBRIGATORIAMENTE ESTAR NA PASTA RAIZ DO USUÁRIO NO HDFS.

# INICIO

## Definições de variáveis e processos iniciais

def valida_nulos(var_nome_campo, var_nome_tabela, var_nome_schema):
    '''Verifica se campo da tabela possui valor NULL
    Argumentos de Entrada:
    var_nome_campo - Formato String - nome do campo a verificar
    var_nome_tabela - Formato String - nome da tabela 
    var_nome_schema - Formato String - nome do schema (database)
    Retorno:
    result - Formato xxx - resultado da verficação, podendo ser:
           None - Quando não há nulos
           99 - quantidade numérica de nulos encontrados
    Exemplo de chamada:
    var_resultado = valida_nulos("dt_ref","tb_limite","work_pfin")
    '''
    vCampo = var_nome_campo
    vTabela = var_nome_tabela
    vDB = var_nome_schema
    if vDB == '':
        query01 = ('select sum(case when ' + vCampo + ' is NULL then 1 else 0 end) as null_indicator from ' +  vTabela + '')
    else:
        query01 = ('select sum(case when ' + vCampo + ' is NULL then 1 else 0 end) as null_indicator from ' + vDB +'.'+ vTabela + '')
    print(query01)
    result = spark.sql(query01).first()[0]
    
    return result

def grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg):
    try:
        '''Função para gravar o status de cada paragrafo de um notebook executado.
        PARAMETROS:
        var_nome_notebook - nome do notebook que esta sendo executado
        var_nome_tabela_indicadores - nome da tabela que esta sendo gerada
        var_seq - sequencia do paragrafo
        var_status - status do paragrafo processado
        var_msg - mensagem
        '''
        vProcesso  = var_nome_notebook
        vTabela    = var_nome_tabela_indicadores
        vSequencia = var_seq
        vStatus    = var_status
        vMsg       = var_msg

        sql_cria_tb_log = ('''create table if not exists work_pfin.tb_log_processamento
                                           (DATA_PROCESSAMENTO       timestamp
                                           ,PROCESSO                 string
                                           ,TABELA                   string
                                           ,SEQUENCIA                int
                                           ,STATUS                    string
                                           ,MSG                       string
                                           ) stored as orc''')
        spark.sql(sql_cria_tb_log)
        
        spark.sql('''
            INSERT INTO WORK_PFIN.TB_LOG_PROCESSAMENTO
            VALUES(
               from_utc_timestamp(current_timestamp(),'GMT-3')
              ,"{vProcesso}"
              ,"{vTabela}"
              ,{vSequencia}
              ,"{vStatus}"
              ,"{vMsg}"
            )
            '''.format(vProcesso=vProcesso,
                       vTabela=vTabela,
                       vSequencia=vSequencia,
                       vStatus=vStatus,
                       vMsg=vMsg))
    except Exception as e:
        raise spark.sql('''INSERT INTO WORK_PFIN.TB_LOG_PROCESSAMENTO
                           VALUES(from_utc_timestamp(current_timestamp(),'GMT-3')
                                 ,"{vProcesso}"
                                 ,"TB_LOG_PROCESSAMENTO"
                                 ,01
                                 ,"ERRO"
                                 ,"#E# OCORREU ERRO NA GRAVACAO DO ERRO ORIGINAL. VERIFIQUE O NOTEBOOK EM EXECUCAO NO MOMENTO DO ERRO"
                                 '''.format(vProcesso=vProcesso))

def valida_lista_campos(lst_de_campos,table_t,var_seq):
    '''
    Função para validar em uma lista de campos de uma tabela se há valores nulos.
    Parâmetros de entrada:
    lst_de_campos - Formato String - lista de campos com as devidas formatações
    table_t - Formato String - nome da tabela
    var_seq - numero de sequencia do paragrafo de execução
    Retorno:
    ind_erro - Formato Boolean - Pode retornar:
       True - quando houver NULL em pelo menos um coluna
       False - quando não houver NULL em nenhum campo
    '''
    ind_erro = False
    lst_campos_a_validar = []
    lst_campos_a_validar = lst_de_campos
    temp_full_tablename = table_t
    var_seq = var_seq
    var_status = 'OK'
    
    for var in lst_campos_a_validar:
    
        RESULTADO = valida_nulos(var,temp_full_tablename,'')
    
        if  RESULTADO > 0:
            var_status = 'ERRO'
            var_msg = '#E# VALIDACAO DE NULOS. EXISTE NULL NO CAMPO ' + var + '.'
            ind_erro = True
        else:
            var_msg = '# VALIDACAO DE NULOS. NAO EXISTE NULL NO CAMPO ' + var + '.'
        grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

    return ind_erro

dt_ref = spark.sql('SELECT date_format(current_date(),"y-MM-01") as dt_ref ').first()[0]


dt_ini_safra = spark.sql('SELECT date_format(add_months(current_date(),-12),"y-MM") as dt_ini_safra ').first()[0]


dt_fim_safra = spark.sql('SELECT date_format(add_months(current_date(),-1 ),"y-MM") as dt_fim_safra ').first()[0]

var_nome_notebook = 'INGESTAO_INSUMOS'
var_nome_tabela_indicadores = 'TB_INS_TESTE'
ind_erro = False

print('dt_ref =======> ' + dt_ref)
print('dt_ini_safra => ' + str(dt_ini_safra))
print('dt_fim_safra => ' + str(dt_fim_safra))

var_seq = 0
var_status = 'OK'
var_msg = '#---- INICIO DO PROCESSO DE INGESTAO DE CSV. SAFRA ' + dt_ref + '.'
grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

var_nome_arquivo = 'arq_csv_teste.txt'
usuario_executor = 'DELAGO'

var_seq = 01
var_status = 'OK'
var_msg = '# PARAMETROS DE ENTRADA. ARQUIVO_CSV ' + var_nome_arquivo + ' usuario ' + usuario_executor + '.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)


var_table_to_drop = ''

if  var_nome_arquivo == 'insumos_faturas.txt':
    var_table_to_drop = 'TB_INS_DADOS_FATURA'
elif var_nome_arquivo == 'insumos_cadastral.txt':
    var_table_to_drop = 'TB_INS_DADO_CADASTRAL'
elif var_nome_arquivo == 'insumos_pgto_fatura.txt':
    var_table_to_drop = 'TB_INS_PGTO_FATURA'
elif var_nome_arquivo == 'insumos_seguros.txt':
    var_table_to_drop = 'TB_INS_SEGUROS'
elif var_nome_arquivo == 'insumos_banca.txt':
    var_table_to_drop = 'TB_INS_BANCA'
elif var_nome_arquivo == 'insumos_rf.txt':
    var_table_to_drop = 'TB_INS_RF'
elif var_nome_arquivo == 'insumos_ge.txt':
    var_table_to_drop = 'TB_INS_GE'
elif var_nome_arquivo == 'insumos_contr_ep.txt':
    var_table_to_drop = 'TB_INS_CONTRCAO_EP'
else: 
    var_table_to_drop = 'TB_INS_TESTE'

query_drop = ('DROP TABLE if exists ' + var_table_to_drop + '')

spark.sql(query_drop)
var_seq = 02
var_status = 'OK'
var_msg = '# TABELA ' + var_table_to_drop + ' EXCLUIDA.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
print(var_msg)


# Tabelas do Processo de faturas

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_DADOS_FATURA
(
   SK_CLIENTE                 bigint       
  ,NUM_CPF                    bigint       
  ,SK_FATURA_CCRED            bigint       
  ,COD_FATURA_CCRED_SO        bigint       
  ,NUM_CONTA_CARTAO_CREDITO   bigint       
  ,DAT_VENCTO_FATURA_CCRED    date         
  ,VAL_SLD_ANT_FATURA         decimal(15,2)
  ,VAL_SLD_ATUAL_FATURA       decimal(15,2)
  ,VAL_PAGAMENTO              decimal(15,2)
  ,VAL_LIM_CRED_BAND          bigint       
  ,VAL_LIMITE_PARCELADO       bigint       
   ) stored AS ORC''')
print('#--- TB_INS_DADOS_FATURA criada. ---#')
print('#')

spark.sql('''
CREATE TABLE if not exists WORK_PFIN.TB_INS_DADO_CADASTRAL
( SK_CLIENTE                bigint
 ,NUM_CPF                   bigint
 ,NUM_CONTA_CARTAO_CREDITO  bigint
 ,SITUACAO_CARTAO           string
 ,LOJA_CADASTRO             string
 ,DATA_CADASTRO             date
 ,DT_CANCELAMENTO           string
 ,MOTIVO_CANCELAMENTO       string
 ,COD_TIPO_CARTAO           string
 ,TIPO_CARTAO               string
 ,DAT_PRIMEIRA_ATIVACAO     string
 ,QTD_ADICIONAIS            string
 ) STORED AS ORC''')

print('#--- TB_INS_DADO_CADASTRAL criada. ---#')
print('#')

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_PGTO_FATURA
( DAT_DIA_PAGTO              date
 ,SK_CLIENTE                 bigint
 ,NUM_CONTA_CARTAO_CREDITO   bigint
 ,COD_FATURA_CCRED_SO        bigint
 ,VAL_PAGTO                  decimal(14,2)
 ,SK_PRODUTO_CARTAO_CREDITO  string
 ,NUM_CPF                    bigint
) STORED AS ORC''')

print('#--- TB_INS_PGTO_FATURA criada. ---#')
print('#')
# Tabela do processo ON-US

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_ON_US_COMPRAS_ANALITICAS
 (sk_cliente           bigint
 ,sk_contrato          bigint
 ,dat_hora_emissao     timestamp
 ,dat_dia              timestamp
 ,sk_local             bigint
 ,cod_estabelecimento  bigint
 ,nom_estabelecimento  string
 ,sk_produto           bigint
 ,des_artigo           string
 ,des_departamento     string
 ,des_secao            string
 ,qtd_artigo           decimal(9,3)
 ,val_preco_cadastro   decimal(9,2)
 ,val_tot_item         decimal(15,2)
 ,dt_criacao           timestamp
 ) STORED AS ORC''')

print('#--- TB_INS_ON_US_COMPRAS_ANALITICAS criada. ---#')
print('#')

#---- Tabelas do processo de seguros

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_SEGUROS
(num_bilhete      bigint
,cpf              bigint
,cod_produto      bigint
,desc_produto     string
,dt_adesao        date
,dt_cancelamento  date
,valor_produto    string
,origem           string
) STORED AS ORC''')
print('#--- TB_INS_SEGUROS criada. ---#')
print('#')

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_BANCA
(SK_CLIENTE            BIGINT
,NUM_BILHETE           string
,SK_PROD_FINANCEIRO    BIGINT
,DESC_PRODUTO          string
,DAT_ADESAO_PFIN       date
,DAT_CANCELAMENTO_PFIN date
,VAL_ADESAO_PFIN       DECIMAL(14,2)
,TIP_ORIGEM            string
,ORIGEM                string
) STORED AS ORC
''')
print('#--- TB_INS_BANCA criada. ---#')
print('#')

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_RF
(SK_CLIENTE              BIGINT
,NUM_BILHETE             string
,SK_PROD_FINANCEIRO      BIGINT
,DESC_PRODUTO            string
,DAT_ADESAO_PFIN         date
,DAT_CANCELAMENTO_PFIN   date
,QTD_MESES_RF            BIGINT
,VAL_ADESAO_PFIN         DECIMAL(14,4)
,ORI_VAL_ADESAO_PFIN     string
,ORIGEM                  string
)stored as orc
''')
print('#--- TB_INS_RF criada. ---#')
print('#')

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_GE
(SK_CLIENTE              BIGINT
,SK_PROD_FINANCEIRO      BIGINT
,DESC_PRODUTO            STRING
,DAT_DIA_GARANTIA        DATE
,DAT_DIA_CANCELAMENTO    DATE
,QTD_MESES_GARANTIA      BIGINT
,VAL_VENDA_GARANTIA      DECIMAL(16,4)
,ORIGEM                  STRING
)STORED AS ORC
''')
print('#--- TB_INS_GE criada. ---#')
print('#')

spark.sql('''CREATE TABLE if not exists WORK_PFIN.TB_INS_CONTRCAO_EP
(SK_CLIENTE             BIGINT
,DAT_DIA_EMPRESTIMO     DATE
,SK_CONTRATO            BIGINT
,COD_PLANO              BIGINT
,VAL_EMPRESTIMO         DECIMAL(14,2)
,ORIGEM                 STRING
)STORED AS ORC''')

print('#--- TB_INS_CONTRCAO_EP criada. ---#')
print('#')

var_seq = 03
var_status = 'OK'
var_msg = '# TABELAS DE INSUMO CRIADAS.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

# BIANCA
path_bia = "hdfs:/user/820742/"

# GABI
path_gabi = "hdfs:/user/347105/"

# RODRIGO
path_rodrigo = "hdfs:/user/357045/"

# DELAGO
path_delago = "hdfs:/user/730217/"

var_seq = 04
var_status = 'OK'
var_msg = '# PATHS PADRÕES DEFINIDO.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

print(var_msg)

var_seq = 05
var_status = 'OK'

if (usuario_executor == 'BIA'):
   path = path_bia
   print(path)
elif (usuario_executor == 'GABI'):
    path = path_gabi
    print(path)
elif (usuario_executor == 'RODRIGO'):
    path = path_rodrigo
    print(path)
elif (usuario_executor == 'DELAGO'):
    path = path_delago
    print(path)

else:
    var_msg = ('#E# USUARIO NAO CADASTRADO!')
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    stop



var_msg = '# VALOR DO PATH DEFINIDO! ' + path
grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

print(var_msg)


#---- Usado para TESTE
v_campos_teste_csv = str("cast(sk_cliente as bigint) as SK_CLIENTE" +
                         ",NOME_CLIENTE " +
                         ",to_date(DT_NASCIMENTO,'dd/MM/yyyy') as DT_NASCIMENTO")

v_campos_cadastral = str("cast(SK_CLIENTE as bigint)  as SK_CLIENTE" + 
 ",cast(NUM_CPF as bigint) as NUM_CPF " +
 ",cast(NUM_CONTA_CARTAO_CREDITO as bigint) as NUM_CONTA_CARTAO_CREDITO" +
 ",SITUACAO_CARTAO" +
 ",LOJA_CADASTRO" +
 ",to_date(DATA_CADASTRO,'dd/MM/yyyy') as DATA_CADASTRO" +
 ",to_date(DT_CANCELAMENTO,'dd/MM/yyyy') as DT_CANCELAMENTO" +
 ",MOTIVO_CANCELAMENTO" +
 ",COD_TIPO_CARTAO" +
 ",TIPO_CARTAO" +
 ",to_date(DAT_PRIMEIRA_ATIVACAO,'dd/MM/yyyy') as DAT_PRIMEIRA_ATIVACAO" +
 ",cast(QTD_ADICIONAIS as bigint) as QTD_ADICIONAIS")

#--- Usados para montagem da TB_DADOS_PAGTO_MOD_ELO
v_campos_faturas = str(" cast(SK_CLIENTE          as bigint) as SK_CLIENTE " +
",cast(NUM_CPF             as bigint) as NUM_CPF " +
",cast(SK_FATURA_CCRED     as bigint) as SK_FATURA_CCRED " +
",cast(COD_FATURA_CCRED_SO as bigint) as COD_FATURA_CCRED_SO " +
",cast(replace(NUM_CONTA_CARTAO_CREDITO,'A','') as bigint) as NUM_CONTA_CARTAO_CREDITO " +
",cast(DAT_VENCTO_FATURA_CCRED as date) as DAT_VENCTO_FATURA_CCRED " +
",cast(VAL_SLD_ANT_FATURA   as decimal(15,2)) as VAL_SLD_ANT_FATURA " +
",cast(VAL_SLD_ATUAL_FATURA as decimal(15,2)) as VAL_SLD_ATUAL_FATURA " +
",cast(VAL_PAGAMENTO        as decimal(15,2)) as VAL_PAGAMENTO " +
",cast(VAL_LIM_CRED_BAND    as bigint       ) as VAL_LIM_CRED_BAND " +
",cast(VAL_LIMITE_PARCELADO as bigint       ) as VAL_LIMITE_PARCELADO ")
 
v_campos_pgto_fatura = str(" to_date(DAT_DIA_PAGTO,'dd/MM/yyyy') as DAT_DIA_PAGTO " +
 ",cast(SK_CLIENTE as bigint) as SK_CLIENTE " +
 ",cast(replace(NUM_CONTA_CARTAO_CREDITO,'A','') as bigint) as NUM_CONTA_CARTAO_CREDITO " +
 ",cast(COD_FATURA_CCRED_SO as bigint) as COD_FATURA_CCRED_SO " +
 ",cast(VAL_PAGTO as decimal(14,2)) as VAL_PAGTO " + 
 ",SK_PRODUTO_CARTAO_CREDITO " +
 ",cast(NUM_CPF as bigint) as NUM_CPF ")

#---- Usado para montagem da TB_SEGUROS_MOD_ELO
v_campos_seguros = str("cast(NUM_BILHETE as BIGINT) as NUM_BILHETE " +
",cast(CPF         as BIGINT) as CPF " +
",cast(COD_PRODUTO as BIGINT) as COD_PRODUTO " +
",DESC_PRODUTO " +
",to_date(DT_ADESAO,'dd/MM/yyyy')       as DT_ADESAO " +
",to_date(DT_CANCELAMENTO,'dd/MM/yyyy') as DT_CANCELAMENTO " +
",VALOR_PRODUTO " +
",ORIGEM ")

v_campos_banca = str("cast(SK_CLIENTE as BIGINT) as SK_CLIENTE " +
  ",NUM_BILHETE " +
  ",cast(SK_PROD_FINANCEIRO as BIGINT) as SK_PROD_FINANCEIRO " +
  ",DESC_PRODUTO " +
  ",to_date(DAT_ADESAO_PFIN,'dd/MM/yyyy') as DAT_ADESAO_PFIN " +
  ",to_date(DAT_CANCELAMENTO_PFIN,'dd/MM/yyyy') AS DAT_CANCELAMENTO_PFIN " +
  ",cast(VAL_ADESAO_PFIN as DECIMAL(14,2)) as VAL_ADESAO_PFIN " +
  ",TIP_ORIGEM " +
  ",ORIGEM ")

v_campos_rf = str("cast(SK_CLIENTE as BIGINT) as SK_CLIENTE " +
",NUM_BILHETE " +
",(SK_PROD_FINANCEIRO as BIGINT) as SK_PROD_FINANCEIRO " +
",DESC_PRODUTO " +
",to_date(DAT_ADESAO_PFIN,'dd/MM/yyyy') as DAT_ADESAO_PFIN " +
",to_date(DAT_CANCELAMENTO_PFIN,'dd/MM/yyyy') as DAT_CANCELAMENTO_PFIN " +
",cast(QTD_MESES_RF as BIGINT) as QTD_MESES_RF " +
",cast(VAL_ADESAO_PFIN as DECIMAL(14,4)) as VAL_ADESAO_PFIN " +
",ORI_VAL_ADESAO_PFIN " +
",ORIGEM ")

v_campos_ge = str("CAST(SK_CLIENTE AS BIGINT) AS SK_CLIENTE " +
",CAST(SK_PROD_FINANCEIRO AS BIGINT) AS SK_PROD_FINANCEIRO " +
",DESC_PRODUTO " +
",to_date(DAT_DIA_GARANTIA,'dd/MM/yyyy') as DAT_DIA_GARANTIA " +
",to_date(DAT_DIA_CANCELAMENTO,'dd/MM/yyyy') as DAT_DIA_CANCELAMENTO " +
",cast(QTD_MESES_GARANTIA AS INT) as QTD_MESES_GARANTIA " +
",cast(VAL_VENDA_GARANTIA AS DECIMAL(16,4)) as VAL_VENDA_GARANTIA " +
",ORIGEM ")

v_campos_ep_contratacao = str("cast(SK_CLIENTE AS BIGINT) as SK_CLIENTE " +
",to_date(DAT_DIA_EMPRESTIMO,'dd/MM/yyyy') AS DAT_DIA_EMPRESTIMO " +
",cast(SK_CONTRATO AS BIGINT) as SK_CONTRATO " +
",cast(COD_PLANO AS BIGINT) as COD_PLANO " +
",cast(VAL_EMPRESTIMO AS DECIMAL(14,2)) as VAL_EMPRESTIMO " +
",ORIGEM ")

var_seq = 06
var_status = 'OK'
var_msg = '# DEFINIDAS FORMATACOES DOS CAMPOS.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)


dict_arquivos_tabela = {'insumos_cadastral.txt':'TB_INS_DADO_CADASTRAL' 
                       ,'insumos_faturas.txt':'TB_INS_DADOS_FATURA'
                       ,'insumos_pgto_fatura.txt':'TB_INS_PGTO_FATURA'
                       ,'insumos_seguros.txt':'TB_INS_SEGUROS'
                       ,'insumos_banca.txt':'TB_INS_BANCA'
                       ,'insumos_rf.txt':'TB_INS_RF'
                       ,'insumos_ge.txt':'TB_INS_GE'
                       ,'insumos_contr_ep.txt':'TB_INS_CONTRCAO_EP'
                       ,'arq_csv_teste.csv':'TB_INS_TESTE'
}

dict_arquivos_schema = {
      'insumos_cadastral.txt':v_campos_cadastral
     ,'insumos_faturas.txt':v_campos_faturas
     ,'insumos_pgto_fatura.txt':v_campos_pgto_fatura
     ,'insumos_seguros.txt':v_campos_seguros
     ,'insumos_banca.txt':v_campos_banca
     ,'insumos_rf.txt':v_campos_rf
     ,'insumos_ge.txt':v_campos_ge
     ,'insumos_contr_ep.txt':v_campos_ep_contratacao
     ,'arq_csv_teste.csv':v_campos_teste_csv
}
var_seq = 07
var_status = 'OK'
var_msg = '# DICIONARIOS DE ARQUIVO-TABELA-LAYOUT DEFINIDOS.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

var_chaves = dict_arquivos_tabela.keys()
print(var_chaves)

var_seq = '08'
var_status = 'OK'
var_msg = '# EXIBIR CHAVES CADASTRADAS NO DICIONARIO ARQUIVO-TABELA.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

#var_campos_tabela = ''
#path = 'hdfs:/user/delago/'
var_seq = '09'

if var_nome_arquivo in dict_arquivos_tabela:
    print(dict_arquivos_tabela[var_nome_arquivo])
    v_tabela = dict_arquivos_tabela[var_nome_arquivo]
    
    var_campos_tabela = dict_arquivos_schema[var_nome_arquivo]

    path_csv = (path + var_nome_arquivo)
    var_status = 'OK'
    var_msg = '# ARQUIVO INFORMADO VALIDO. ' + var_nome_arquivo
    print(path_csv)
    print('#----#')
    print(var_campos_tabela)
else:
    var_status = 'ERRO'
    var_msg = ('#E# ARQUIVO INFORMADO NAO CADASTRADO NOS DICIONARIOS. ' + var_nome_arquivo)
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    print(var_msg)
    
    var_msg='#E# VERIFIQUE SE O NOME DO ARQUIVO ESTA CORRETO OU CADASTRE NO DICIONARIO DE TRATAMENTOS DO PROCESSO.'
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    print(var_msg)
    
    var_msg='#E#--- PROCESSO INTERROMPIDO!! ---#'
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    print(var_msg)
    sys.exit()

#grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)


full_tablename = str("WORK_PFIN.") + v_tabela
temp_full_tablename = str("TEMP_") + v_tabela

print(full_tablename)
print(temp_full_tablename)

var_seq = 10
var_status = 'OK'
var_msg = '# NOMES DAS TABELAS DEFINIDAS. ' + full_tablename + ' / ' + temp_full_tablename + ' .'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)


var_seq = 11

try:
    df = spark.read.csv(path=path_csv,sep=";",header=True)
    print("# QTD LINHAS ==> " + str(df.count()))
    print("# QTD COLUNAS => " + str(len(df.columns)))
    df.printSchema()
    df.registerTempTable(temp_full_tablename)
    var_status = 'OK'
    var_msg = '# LEITURA DO CSV EFETUADA. ' + path_csv + ''
except Exception:
    var_status = 'ERRO'
    var_msg = '#E# OCORREU ERRO NA LEITURA DO CSV.'
    ind_erro = True
finally:
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    # PODE SER EXCLUIDO DAQUI PARA BAIXO
    spark.sql('describe '+ temp_full_tablename).show()
    spark.sql('select * from '+ temp_full_tablename + ' limit 1').show(truncate=False)

if ind_erro is True:
    sys.exit()


var_status = 'OK'
var_seq = 12
var_msg = '# VALIDACAO DE NULOS EFETUADA.'

if  var_nome_arquivo == 'arq_csv_teste.csv':
    #lst_campos_formatados = [" cast(sk_cliente as bigint) "," to_date(dt_nascimento,'dd/MM/yyyy') "]
    lst_campos_formatados = [" NOME_CLIENTE "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_cadastral.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as bigint) "
                            ," cast(NUM_CPF as bigint) "
                            ," cast(NUM_CONTA_CARTAO_CREDITO as bigint) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_faturas.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as bigint) "
                            ," cast(NUM_CPF as bigint) "
                            ," cast(COD_FATURA_CCRED_SO as bigint) "
                            ," cast(replace(NUM_CONTA_CARTAO_CREDITO,'A','') as bigint) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_pgto_fatura.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as bigint) "
                            ," cast(replace(NUM_CONTA_CARTAO_CREDITO,'A','') as bigint) "
                            ," cast(COD_FATURA_CCRED_SO as bigint) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_seguros.txt':
    lst_campos_formatados = [" cast(CPF as BIGINT) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_banca.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as BIGINT) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_rf.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as BIGINT) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)

elif var_nome_arquivo == 'insumos_ge.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as BIGINT) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)
    
elif var_nome_arquivo == 'insumos_contr_ep.txt':
    lst_campos_formatados = [" cast(SK_CLIENTE as BIGINT) "]
    ind_erro = valida_lista_campos(lst_campos_formatados,temp_full_tablename,var_seq)
else:
    var_status = 'ERRO'
    var_msg = '#E# NOME DO ARQUIVO NAO ENCONTRADO ' + var_nome_arquivo + '.'
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)
    ind_erro = True

if ind_erro is True:
    sys.exit()
else:
    grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

var_seq = 13
var_status = 'OK'

#var_campos_tabela = v_campos_ep_contratacao

var_query = ''
var_query = ("INSERT INTO "+ full_tablename + " SELECT "+ var_campos_tabela + " FROM "+ temp_full_tablename + "")

print(var_query)
spark.sql(var_query)

var_msg = '# TABELA ' + var_table_to_drop + ' GRAVADA.'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

var_seq = 14
var_status = 'OK'
var_msg = '#--- PROCESSO FINALIZADO ---#'

grava_log(var_nome_notebook, var_nome_tabela_indicadores, var_seq, var_status, var_msg)

spark.sql('''
select * from work_pfin.tb_log_processamento
order by data_processamento desc
limit 20
''').show(truncate=False)