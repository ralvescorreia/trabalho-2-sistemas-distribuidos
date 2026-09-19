import asyncio
import websockets
import json
import re

# Dicionários e listas para armazenar o estado global
clientes_conectados = {}
todos_os_eventos = []
processos_finalizados = set()
TOTAL_PROCESSOS = 3

def extrair_participante(tipo, detalhes):
    """
    Função auxiliar para extrair o nome do outro processo (P1, P2, P3)
    a partir do texto de detalhes do log original.
    Usa Expressões Regulares (Regex) para encontrar o padrão 'P' seguido de número.
    """
    if tipo == "EXEC":
        return ""
    
    # Procura por P1, P2 ou P3 no texto dos detalhes
    match = re.search(r'P[1-3]', detalhes)
    if match:
        return match.group(0) # Retorna o texto encontrado (ex: "P3")
    return "?"

def exibir_listagem_por_processo():
    """
    Imprime os logs agrupados por processo, mantendo a ordem local,
    e incluindo quem enviou ou recebeu a mensagem.
    """
    print("\n" + "="*80)
    print("VISÃO LOCAL: LISTAGEM SEQUENCIAL POR PROCESSO (COM ORIGEM/DESTINO)")
    print("="*80)
    
    ids_processos = ["P1", "P2", "P3"]
    
    for pid in ids_processos:
        # Filtra e ordena os eventos deste processo pelo timestamp local
        eventos_do_processo = [e for e in todos_os_eventos if e["processo_id"] == pid]
        eventos_ordenados_locais = sorted(eventos_do_processo, key=lambda item: item["timestamp"])
        
        if eventos_ordenados_locais:
            print(f"\n--- Histórico de {pid} ---")
            for e in eventos_ordenados_locais:
                tipo = e['tipo']
                timestamp = e['timestamp']
                detalhes_originais = e['detalhes']
                
                # Formatação condicional baseada no tipo de evento
                if tipo == "SEND":
                    destino = extrair_participante(tipo, detalhes_originais)
                    print(f"{pid} send para {destino} relógio lógico {timestamp}")
                
                elif tipo == "RECEIVE":
                    origem = extrair_participante(tipo, detalhes_originais)
                    print(f"{pid} receive de {origem} relógio lógico {timestamp}")
                
                else: # EXEC
                    print(f"{pid} exec relógio lógico {timestamp}")
        else:
            print(f"\n--- {pid} não registrou eventos ---")

    print("\n" + "="*80 + "\n")


def exibir_lista_unificada():
    """
    Aplica a ordenação total (Requisito 3) e imprime a lista global formatada.
    """
    print("="*80)
    print("VISÃO GLOBAL: LISTA UNIFICADA (ORDENAÇÃO TOTAL DE LAMPORT)")
    print("Critério: Menor Timestamp -> Desempate por ID do Processo")
    print("="*80)
    
    # Ordena: 1º por timestamp, 2º por ID do processo (P1 < P2 < P3)
    eventos_ordenados = sorted(
        todos_os_eventos, 
        key=lambda item: (item["timestamp"], item["processo_id"])
    )
    
    for e in eventos_ordenados:
        # Usa a formatação completa e detalhada exigida no enunciado
        print(f"[Processo {e['processo_id']}] Evento: {e['tipo']} | Relógio Lógico: {e['timestamp']} | Detalhes: {e['detalhes']}")
    print("="*80 + "\n")


async def roteador(websocket):
    # Fase de handshake: recebe o ID do processo ao conectar
    try:
        id_processo = await websocket.recv()
    except websockets.exceptions.ConnectionClosed:
        return

    clientes_conectados[id_processo] = websocket
    print(f"[Servidor] Processo {id_processo} conectado.")

    try:
        async for mensagem in websocket:
            dados = json.loads(mensagem)
            tipo_pacote = dados.get("tipo_pacote")
            
            # Processa pacote de finalização (coleta de logs)
            if tipo_pacote == "FINALIZADO":
                eventos_recebidos = dados.get("eventos", [])
                todos_os_eventos.extend(eventos_recebidos)
                processos_finalizados.add(id_processo)
                print(f"[Servidor] Relatório recebido de {id_processo}. ({len(processos_finalizados)}/{TOTAL_PROCESSOS})")
                
                # Quando todos terminarem, exibe os resultados
                if len(processos_finalizados) >= TOTAL_PROCESSOS:
                    exibir_listagem_por_processo() # Visão local corrigida
                    exibir_lista_unificada()      # Visão global (Req 3)
            
            # Processa roteamento de mensagens entre processos
            else:
                destino = dados.get("destino")
                if destino in clientes_conectados:
                    await clientes_conectados[destino].send(json.dumps(dados))
                else:
                    print(f"[Servidor] Destino {destino} indisponível (tentativa de {id_processo}).")
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Cleanup ao desconectar
        if id_processo in clientes_conectados:
            del clientes_conectados[id_processo]
        print(f"[Servidor] Processo {id_processo} desconectado.")


async def main():
    # Inicia o servidor na porta 8765
    async with websockets.serve(roteador, "localhost", 8765):
        print("[Servidor] Rodando na porta 8765. Aguardando processos...")
        await asyncio.Future() # Roda para sempre

if __name__ == "__main__":
    # Import necessário para a função extrair_participante
    import re
    asyncio.run(main())