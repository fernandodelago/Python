#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''bibliotecas utilizadas'''
import json
from sseclient import SSEClient as EventSource
import pandas as pd


# In[2]:


'''Origem do dado - Wikipedia'''
url = 'https://stream.wikimedia.org/v2/stream/recentchange'


# In[ ]:


'''Faz captura de mensagens (formato JSON) e armazena em um arquivo .txt
   Há necessidade de criar 2 arquivos no mesmo diretório do programa .py: 
       req.txt
       saida.txt
   A captura de mensagem será feita até que o arquivo de parâmetro (req.txt) 
   seja alterado para "False". 
   Por DEFAULT o parâmetro de captura é "True".
   O arquivo de saída é o saida.txt.'''
contador = 0
gravacao_d = {}
gravacao_d['lista'] = []

for event in EventSource(url):
    if event.event == 'message':
        try:
            df_req_pd = pd.read_csv("req.txt")
            if 'True' in df_req_pd:
                change = json.loads(event.data)
                if change['bot'] == True:
                    contador += 1
                    print("contador = " + str(contador))
                    print("#-----------------------------------------#")
                    print(change)
                    
                    gravacao_d['lista'].append({**change})
                    
                    with open("saida.txt","w",encoding='utf8') as txtfile:
                        json.dump(gravacao_d,txtfile)
            elif 'False' in df_req_pd:
                print("#-----------------------------------------#")
                print("Registros gravados => " + str(contador))
                break

        except ValueError:
            continue
            print("Ocorreu um erro!!!")


# In[ ]:




