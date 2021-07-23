# informe o caminho do mount point
mount_point = '/mnt/dev/'

try:
  par_path_datalake = dbutils.widgets.get("par_path_datalake")
  print('# Execução via ADF')
  print('#')
except:
  print('# Erro ou execução fora do ADF')
  print('#')
  print('# par_path_datalake => ' + par_path_datalake)
  dbutils.notebook.exit(1)

print('# par_path_datalake => ' + par_path_datalake)
path_file_datalake = mount_point + par_path_datalake
print('# path_file_datalake => ' + path_file_datalake)

try:
  par_nome_tabela = dbutils.widgets.get("par_nome_tabela")
  print('# Execução via ADF')
  print('#')
except:
  par_nome_tabela = 'tabela nao informada'
  print('# Erro ou execução fora do ADF')
  print('# par_nome_tabela => ' + par_nome_tabela)
  print('#')
  dbutils.notebook.exit(1)

print('# par_nome_tabela => ' + par_nome_tabela)

try:
  par_filename_extraction = dbutils.widgets.get("par_filename_extraction")
  print('# Execução via ADF')
  print('#')
except:
  par_filename_extraction = 'arquivo nao informado'
  print('# Erro ou execução fora do ADF')
  print('# par_filename_extraction => ' + par_filename_extraction)
  print('#')
  dbutils.notebook.exit(1)

print('# par_filename_extraction => ' + par_filename_extraction)

def grava_parquet(df, path_to_write, nome_arquivo):
  """
  Função para gravar em arquivo CSV único.
  ---
  (EN)Function to save data in only one CSV file.
  ---
  Entrada (Input):
   df - dataframe spark.
   path_to_write - path onde deve ser gravado o arquivo .csv.
   nome_arquivo - nome do arquivo que deve ser gravado (com extensão!).
   
  Saída (Output)..:
   return_code - codigo de returno da chamada.
     |-> 0 - Processamento com sucesso.
     |-> 1 - Path nao informado.
     |-> 2 - Nome do arquivo a gravar nao informado.
     |-> 3 - Separador de campos nao informado.
   msg - mensagem informativa.
  ----
  Example chamada (Example Call):
  grava_csv(df, '/mnt/files', 'my_output.csv', ';')
  """
  try:
    #path_to_write = path_to_write
    #nome_arquivo = nome_arquivo
    #sep = sep
    if not path_to_write:
      msg = '#E# Path nao informado.'
      return_code = 1
    elif not nome_arquivo:
      msg = '#E# Nome do arquivo a gravar nao informado.'
      return_code = 2
    elif not sep:
      msg = '#E# Separador de campos não informado.'
      return_code = 3
    else:
      var_nome_arquivo = nome_arquivo
      work_dir = (path_to_write + 'tmp/' + var_nome_arquivo)
      dest_file = (path_to_write + var_nome_arquivo )
      print('# Arquivo gravado em ' + dest_file)
      
      # Salva em formato .CSV
      #df.coalesce(1)\
      #    .write.format("csv")\
      #    .mode("overwrite")\
      #    .option("header", "true")\
      #    .options(delimiter='{sep}'.format(sep = sep)) \
      #    .save("{path_save}".format(path_save = work_dir) ,encoding='utf-8')
      
      # Salva em formato .parquet
      df.coalesce(1)\
          .write\
          .mode("overwrite")\
          .parquet("{path_save}".format(path_save = work_dir))
      files = dbutils.fs.ls(work_dir)
      pqt_file = [x.path for x in files if x.path.endswith(".parquet")][0]
      #csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
      # move o arquivo unico para o dest_file
      #dbutils.fs.mv(csv_file, dest_file)
      dbutils.fs.mv(pqt_file, dest_file)
      # remove o diretorio de trabalho
      dbutils.fs.rm(work_dir, recurse = True)
      msg = '# Processamento com sucesso.'
      return_code = 0
  except:
    msg = '#E# Ocorreu um erro inesperado.'
    return_code = 9
  finally:
    return return_code, msg

print('# Buscando dados na tabela ' + par_nome_tabela + '.')
df = spark.sql('''SELECT * FROM {nome_tabela}'''.format(nome_tabela = par_nome_tabela))

print('# Quantidade de linhas selecionadas => ' + str(df.count()))

grava_parquet(df, path_file_datalake, par_filename_extraction)

print('#--- FIM DO PROCESSAMENTO ---#')
