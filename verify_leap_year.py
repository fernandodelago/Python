#! python 3
def verify_leap_year(var_ano):
    """
    Verifica se o ano é bissexto.
    Entrada:
    var_ano - ano que deve ser verificado - formato int
    Retorno:
    rc_funcao - código de retorno de execução - formato int
      Possíveis valores:
        0 - ano é bissexto
        1 - ano não é bissexto
    fim_fev - último dia de Fevereiro do ano consultado - formato int
    """

    from datetime import datetime

    if (var_ano%4==0 and var_ano%100!=0) or (var_ano%400==0):
      # é bissexto
      fim_fev=29
      rc_funcao = 0
    else:
      # não é bissexto
      fim_fev=28
      rc_funcao = 1

    return rc_funcao, fim_fev