import asyncio
import websockets
import json
import sys
import random
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()

class ProcessoLamport:
    def __init__(self, meu_id):
        self.id = meu_id
        self.relogio = 0
        self.historico_estruturado = []

    def registrar_log(self, tipo_evento, detalhes):
        cor_borda = "cyan" if tipo_evento == "SEND" else "green" if tipo_evento == "RECEIVE" else "yellow"

        texto = Text()
        texto.append("Evento: ", style="bold")
        texto.append(f"{tipo_evento:^7}", style=f"bold {cor_borda}")
        texto.append(" | Relógio Lógico: ", style="bold")
        texto.append(f"{self.relogio:02d}", style="bold magenta")
        texto.append(f" | Detalhes: {detalhes}")

        painel = Panel(
            texto, 
            title=f"[bold]Processo {self.id}[/bold]", 
            border_style=cor_borda, 
            expand=False
        )
        console.print(painel)
        
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
        return {
            "tipo_pacote": "MENSAGEM",
            "origem": self.id,
            "destino": destino,
            "conteudo": conteudo,
            "timestamp": self.relogio
        }

    def processar_recebimento(self, origem, conteudo, timestamp_da_mensagem):
        self.relogio = max(self.relogio, timestamp_da_mensagem) + 1
        self.registrar_log("RECEIVE", f"De {origem}: {conteudo}")

# --- A MÁGICA DO ATRASO REAL ---
# Esta função simula a latência da rede voando no ar sem travar o processo
async def simular_viagem_na_rede(meu_lamport, origem, conteudo, timestamp):
    # TEMPO DE VIAGEM LENTO: 8 segundos. Dá tempo de sobra para você apontar na tela
    await asyncio.sleep(8.0) 
    meu_lamport.processar_recebimento(origem, conteudo, timestamp)


async def executar_processo(meu_id):
    uri = "ws://localhost:8765"
    meu_lamport = ProcessoLamport(meu_id)
    
    sinal_inicio = asyncio.Event()
    
    async with websockets.connect(uri) as websocket:
        await websocket.send(meu_id)
        console.print("[dim]Conectado ao servidor. A aguardar outros processos...[/dim]")
        
        async def escutar_mensagens():
            try:
                async for mensagem in websocket:
                    dados = json.loads(mensagem)
                    
                    if dados.get("tipo_pacote") == "INICIAR":
                        console.print("[bold yellow]Todos ligados! A iniciar Lamport...[/bold yellow]\n")
                        sinal_inicio.set()
                        
                    elif dados.get("tipo_pacote") == "MENSAGEM":
                        # Em vez de um sleep que trava tudo, disparamos a viagem em paralelo!
                        asyncio.create_task(simular_viagem_na_rede(
                            meu_lamport,
                            dados.get("origem"), 
                            dados.get("conteudo"), 
                            dados.get("timestamp")
                        ))
            except asyncio.CancelledError:
                pass
                
        async def gerar_eventos_aleatorios():
            await sinal_inicio.wait()
            
            # DESCOMPASSO INICIAL: P1 arranca em 2s, P2 em 6s, P3 em 10s
            atraso_inicial = {"P1": 2.0, "P2": 6.0, "P3": 10.0}.get(meu_id, 3.0)
            await asyncio.sleep(atraso_inicial)
            
            destinos_possiveis = ["P1", "P2", "P3"]
            if meu_id in destinos_possiveis:
                destinos_possiveis.remove(meu_id)
            
            for _ in range(4):
                acao = random.choice(["interno", "enviar"])
                
                if acao == "interno":
                    meu_lamport.executar_evento_interno("Ação automática")
                else:
                    destino = random.choice(destinos_possiveis)
                    msg_pronta = meu_lamport.preparar_envio(destino, "Dados processados")
                    await websocket.send(json.dumps(msg_pronta))
                
                # TEMPO DE RESPIRAÇÃO: 12 segundos entre cada nova ação do terminal
                await asyncio.sleep(12.0)

        tarefa_escuta = asyncio.create_task(escutar_mensagens())
        await gerar_eventos_aleatorios()
        
        # Pausa longa no final para as últimas mensagens da rede chegarem tranquilamente
        await asyncio.sleep(15.0)
        tarefa_escuta.cancel()

        pacote_final = {
            "tipo_pacote": "FINALIZADO",
            "eventos": meu_lamport.historico_estruturado
        }
        await websocket.send(json.dumps(pacote_final))
        console.print(f"\n[bold red][{meu_id}] Execução concluída. Registos enviados ao Servidor.[/bold red]")

if __name__ == "__main__":
    meu_id = sys.argv[1] if len(sys.argv) > 1 else "P1"
    asyncio.run(executar_processo(meu_id))