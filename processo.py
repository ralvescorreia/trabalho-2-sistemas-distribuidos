import asyncio
import websockets
import json
import sys
import random

class ProcessoLamport:
    def __init__(self, meu_id):
        self.id = meu_id
        self.relogio = 0
        self.historico_estruturado = []

    def registrar_log(self, tipo_evento, detalhes):
        # Exibição em tempo real no terminal local
        log_formatado = f"[Processo {self.id}] Evento: {tipo_evento} | Relógio Lógico: {self.relogio} | Detalhes: {detalhes}"
        print(log_formatado)
        
        # Guarda estruturado para a ordenação final
        self.historico_estruturado.append({
            "processo_id": self.id,
            "timestamp": self.relogio,
            "tipo": tipo_evento,
            "detalhes": detalhes
        })

    def executar_evento_interno(self, detalhes="Processamento local"):
        self.relogio += 1
        self.registrar_log("EXEC", detalhes)

    def preparar_envio(self, destino, conteudo):
        self.relogio += 1
        self.registrar_log("SEND", f"Para {destino}: {conteudo}")
        
        mensagem = {
            "tipo_pacote": "MENSAGEM",
            "origem": self.id,
            "destino": destino,
            "conteudo": conteudo,
            "timestamp": self.relogio
        }
        return mensagem

    def processar_recebimento(self, origem, conteudo, timestamp_da_mensagem):
        self.relogio = max(self.relogio, timestamp_da_mensagem) + 1
        self.registrar_log("RECEIVE", f"De {origem}: {conteudo}")

async def executar_processo(meu_id):
    uri = "ws://localhost:8765"
    meu_lamport = ProcessoLamport(meu_id)
    
    async with websockets.connect(uri) as websocket:
        await websocket.send(meu_id)
        
        tarefa_ativa = True
        
        async def escutar_mensagens():
            try:
                async for mensagem in websocket:
                    dados = json.loads(mensagem)
                    if dados.get("tipo_pacote") == "MENSAGEM":
                        meu_lamport.processar_recebimento(
                            dados.get("origem"), 
                            dados.get("conteudo"), 
                            dados.get("timestamp")
                        )
            except asyncio.CancelledError:
                pass
                
        async def gerar_eventos_aleatorios():
            destinos_possiveis = ["P1", "P2", "P3"]
            if meu_id in destinos_possiveis:
                destinos_possiveis.remove(meu_id)
            
            # Simula 4 eventos espaçados no tempo
            for _ in range(4):
                await asyncio.sleep(random.uniform(1.0, 3.0))
                acao = random.choice(["interno", "enviar"])
                
                if acao == "interno":
                    meu_lamport.executar_evento_interno("Ação automática")
                else:
                    destino = random.choice(destinos_possiveis)
                    msg_pronta = meu_lamport.preparar_envio(destino, "Dados processados")
                    await websocket.send(json.dumps(msg_pronta))

        tarefa_escuta = asyncio.create_task(escutar_mensagens())
        await gerar_eventos_aleatorios()
        
        # Pausa breve para receber mensagens em trânsito antes de encerrar
        await asyncio.sleep(2.0)
        tarefa_escuta.cancel()

        # Envia histórico de eventos ao servidor para ordenação global
        pacote_final = {
            "tipo_pacote": "FINALIZADO",
            "eventos": meu_lamport.historico_estruturado
        }
        await websocket.send(json.dumps(pacote_final))
        print(f"[{meu_id}] Execução concluída. Logs enviados ao Servidor.")

if __name__ == "__main__":
    meu_id = sys.argv[1] if len(sys.argv) > 1 else "P1"
    asyncio.run(executar_processo(meu_id))