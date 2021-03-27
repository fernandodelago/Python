from pyspark.sql.functions import col, regexp_replace

# substitui vogais com acentos por sem acentos
# substitui "Ç" por "C"
# caracteres especiais ficarão com valor '#'

acentos_portugues = [
    (u'á', 'a'), (u'Á', 'A'),
    (u'â', 'a'), (u'Â', 'A'),
    (u'à', 'a'), (u'À', 'A'),
    (u'ã', 'a'), (u'Ã', 'A'),
    (u'é', 'e'), (u'É', 'E'),
    (u'ê', 'e'), (u'Ê', 'E'),
    (u'í', 'i'), (u'Í', 'I'),
    (u'ò', 'o'), (u'Ò', 'O'),
    (u'ô', 'o'), (u'Ô', 'O'),
    (u'ó', 'o'), (u'Ó', 'O'),
    (u'ú|ü', 'u'), (u'Ú|Ű', 'U'),
    (u'ñ', 'n'),
    (u'ç', 'c'), (u'Ç', 'C'),
    ('[^\x00-\x7F]', '#') 
]

def remove_acentos(column):
    r = col(column)
    for a, b in acentos_portugues:
        r = regexp_replace(r, a, b)
    return r.alias('remove_acentos(' + column + ')')
