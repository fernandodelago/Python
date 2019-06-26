#!/usr/bin/env python
# coding: utf-8

# Verifica data de referência de carga entre uma lista de tabelas

# In[ ]:


'''#retirar quando for implantar'''
import findspark
findspark.init()
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


# In[ ]:


from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import HiveContext, SparkContext
from datetime import date, datetime


# In[ ]:


sc = SparkContext()


# In[ ]:


hive_context = HiveContext(sc)


# In[ ]:


#test
lista = ['vc_movtos_validos_analitico','vcdt076']
db_name = ['pcp','vc']


# In[ ]:


error_list = [[],[],[]]


# In[ ]:


#error_list = verifica_data_ref_carga(hive_context,lista,database)


# In[ ]:


lista[0]


# In[ ]:


for idx, var in enumerate(lista):
    print(idx)
    print(var)


# In[ ]:


hive_context.sql("""show tables in vc""").show()


# In[ ]:


hive_context.sql("""show tables in pcp""").show()


# In[ ]:


result1 = hive_context.sql("select max(`dat_ref_carga`) as dt_max from vc.vcdt076").collect()


# In[ ]:


result1


# In[ ]:


result2 = hive_context.sql("select max(`dat_ref_carga`) as dt_max from pcp.vc_movtos_validos_analitico").collect()


# In[ ]:


result2


# In[ ]:


datetime.strptime(result1[0].dt_max,'%Y-%m-%d')


# In[ ]:


def verifica_data_ref_carga(hive_context, datetime, lista_de_tabelas, db_name):
    '''Verificada se as datas de carga da lista de tabelas informada é a mesma e retorna OK ou NOK
       Parametros de Entrada:
       hive_context     - contexto do Hive para execução de queries
       datetime         - biblioteca com funções de calculo de data do Python
       lista_de_tabelas - Lista de tabelas à consultar
       db_name          - lista dos databases referenciados as tabelas'''
    
    hc = hive_context
    lista = []
    lista = lista_de_tabelas
    db = []
    db = db_name
    error_list = [[],[],[]]
    
    for idx,var in enumerate(lista):
        result = hc.sql("select max(`dat_ref_carga`) as dt_max from {0}.{1}".format(db[idx],var)).collect()
        
        dt_max_tabela = datetime.strptime(result[0].dt_max,'%Y-%m-%d')
        
        #Carrega valor de dt_anterior na primeira execucao
        if idx == 0:
            dt_base_carga = str(dt_max_tabela.date())
            dt_anterior = dt_max_tabela
            
        if dt_max_tabela.date() != dt_anterior.date():
            #eh erro
            Status = "NOK"
            Msg_detalhada = "Data da carga da tabela diferente das demais. Data da Carga: {}"
        else:
            #esta certo
            Status = "OK"
            Msg_detalhada = "Data da carga da tabela correta. Data da Carga: {}"
        
        error_list[0].append(Status)
        error_list[1].append(var)
        error_list[2].append(Msg_detalhada.format(dt_max_tabela.date()))
        
        dt_anterior = dt_max_tabela

    return error_list, dt_base_carga


# In[ ]:


#test
x = datetime.strptime(result1[0].dt_max,'%Y-%m-%d')


# In[ ]:


#test
print(date.today())
print(x.date())
if x.date() != date.today():
    print('Datas diferentes')
else:
    print('Datas iguais')   


# In[ ]:


error_list_retorno, dt_base = verifica_data_ref_carga(hive_context,datetime,lista,db_name)


# In[ ]:


for idx, var in enumerate(error_list_retorno):
    print("Chave: " + str(idx) + " - Valor: " + str(var))


# In[ ]:


dt_base

