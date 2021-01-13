#! python 3

def mostra_hora():
    '''
    Função simples para imprimir a hora na tela.
    Fuso_horário utilizado - Brazil/Sao_Paulo
    --
    (EN)Simple function to show date and time.
    Timezone used - Brazil/Sao_paulo
    '''
    from datetime import datetime, timedelta, timezone
    diferenca = timedelta(hours = -3)
    fuso_horario = timezone(diferenca)
    dt_hr_agora = datetime.now(fuso_horario)
    print(dt_hr_agora.strftime('%Y-%m-%d %H:%M:%S'))
