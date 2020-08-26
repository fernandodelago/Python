import chardet

# nome_do_arquivo.csv - replace this name for you filename

# a each time read, the chardet.detec will identifying the encoding
with open(r"nome_do_arquivo.csv","rb") as f:
    result = chardet.detect(f.read())
    df=spark.read.csv(r"202003_RecebimentosRecursosPorFavorecido.csv",encoding=result['encoding'],sep=';',header=True)
