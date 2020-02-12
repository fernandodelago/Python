# Função que calcula nova data com base na quantidade de meses

def meses_entre_datas(v_ano_mes,qtd_meses):
    '''
    v_ano_mes - ano e mês (yyyymm) base para o calculo. Formato String.
    qtd_meses - qtd de meses a subtrair ou somar. Para subtrair informar valores negativos. Formato INT.
    Retorno:
    var_new_anomes - novo valor de ano e mês após o cálculo (yyyymm). Formato String.
    '''
    from datetime import date
    
    v_ano_mes = v_ano_mes
    qtd_calcular = qtd_meses
    var_mes = int(v_ano_mes[4:])
    var_ano = int(v_ano_mes[:4])
    
    if '-' in str(qtd_calcular):
        
        if var_mes > (qtd_calcular * -1):
            print('0')
            var_mes_new = var_mes + qtd_calcular
            if var_mes_new == 0:
                var_mes_new = 12
                var_ano_new = var_ano - 1
            else:
                var_ano_new = var_ano
        elif var_mes == qtd_calcular:
            print('1')
            var_mes_new = 12
            var_ano_new = var_ano - 1
        else:
            print('2')
            subtrair = int(qtd_calcular)
            var_ano_new = var_ano
            while subtrair != 0:
                var_mes_new = (var_mes + 12) + subtrair
                var_ano_new = var_ano_new - 1
                if var_mes_new < 1:
                    subtrair = var_mes_new
                else:
                    subtrair = 0
    else:
        print('3')
        var_mes_new = var_mes + qtd_calcular
        while var_mes_new > 12:
            var_ano_new = var_ano + 1
            var_mes_new = var_mes_new - 12
            
    var_new_anomes=str(var_ano_new) + str(var_mes_new).zfill(2)
    return var_new_anomes