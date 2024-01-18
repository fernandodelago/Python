#! python 3
mapping_fuel = {
    'GASÓLEO': 'Combustão', 
    'ENERGIA ELÉCTRICA': 'Elétricos e Plug-Ins', 
    'PLUG-IN GASOLINA': 'Elétricos e Plug-Ins',
    'GASOLINA': 'Combustão', 
    'SEM COMBUSTÍVEL': 'Sem Combustivel', 
    'HÍBRIDO GASOLINA': 'Híbridos', 
    'GASOLINA/GAS NATURAL': 'Combustão', 
    'HÍBRIDO GASÓLEO': 'Híbridos', 
    'GÁS PROPANO GARRAFA': 'Combustão', 
    'PLUG-IN GASÓLEO': 'Elétricos e Plug-Ins'
}
df_carros = df_carros.withColumn(
    'categoria',
    F.udf(lambda via_fuel: mapping_fuel.get(categoria, None), F.StringType() )(F.col('categoria') )
    )
