#! python 3
import pyautogui 

nome_saida = 'contato'
sequencia = 1

def posicao_do_mouse():
    #posicao_do_mouse()
    currentMouseX, currentMouseY = pyautogui.position()
    print('# Posição X => ' + str(currentMouseX))
    print('# Posição Y => ' + str(currentMouseY))

#print('# Tamanho da tela')
#print(pyautogui.size()) 

def pega_contato():
    # posiciona na busca de contatos
    pyautogui.moveTo(109, 218)
    pyautogui.click()
    pyautogui.press('tab')

qtd_contatos = pyautogui.prompt(text='Informe a quantidade de contatos a capturar.', title='Responda', default='')    

pega_contato()
contador = int(qtd_contatos)
while (contador > 0):
    pyautogui.hotkey('Alt','Tab')
    pyautogui.click(interval=2)
    pyautogui.moveTo(553,198, duration = 1)
    pyautogui.click() 
    pyautogui.hotkey('ctrl', 'home')
    pyautogui.sleep(2)
    pyautogui.hotkey('ctrl', 'home')
    pyautogui.sleep(2)
    pyautogui.hotkey('ctrl', 'home')
    pyautogui.sleep(2)
    distance=990

    # arrasta o cursor para seleção
    # coordenadas em formato X,Y (horizontal, vertical)
    pyautogui.drag(0,distance,duration=6)  
    #pyautogui.alert('Seleção foi feita! Iniciar cópia.')

    #fazendo cópia da seleção
    pyautogui.hotkey('ctrl','c')

    # abrindo o executar do Windows
    pyautogui.hotkey('winleft','r')
    pyautogui.sleep(1)

    # abrindo o notepad
    pyautogui.write('notepad')
    pyautogui.sleep(1)
    pyautogui.press('enter')
    pyautogui.sleep(1)

    # colando a seleção encontrada
    pyautogui.hotkey('ctrl','v')

    # salvando a conversa arquivo
    pyautogui.press(['alt','a','r'])
    nome_arquivo = nome_saida + str(sequencia) + '.txt'
    pyautogui.sleep(1)
    print(nome_arquivo)
    print('----------------')
    pyautogui.write(nome_arquivo)
    pyautogui.press('enter')
    pyautogui.sleep(1)

    # aumentando a sequencia do nome do arquivo
    sequencia = sequencia + 1
    # fechando o notepad
    pyautogui.hotkey('alt','f4')

    # subtraindo 1 do contador
    contador =  contador - 1

    pyautogui.sleep(1)
    # pega próximo contato
    pega_contato()
    pyautogui.press('down')

pyautogui.alert('FIM DO PROCESSO.')
#--print('# seleção copiada.')