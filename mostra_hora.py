#! python 3

def mostra_hora():
    '''Função simples para imprimir a hora na tela'''
    from datetime import datetime
    agora = datetime.now()
    print(agora.strftime('%Y-%m-%d %H:%M:%S'))