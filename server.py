import asyncio
import websockets
import json
import re
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns

console = Console()

clientes_conectados = {}
todos_os_eventos = []
processos_finalizados = set()
TOTAL_PROCESSOS = 3

def extrair_participante(tipo, detalhes):
    if tipo == "EXEC":
        return ""
    match = re.search(r'P[1-3]', detalhes)
    if match:
        return match.group(0)
    return "?"

def exibir_listagem_por_processo():
    console.print("\n[bold cyan]" + "="*80 + "[/bold cyan]")
    console.print("[bold white]VISÃO LOCAL: LISTAGEM SEQUENCIAL POR PROCESSO (COM ORIGEM/DESTINO)[/bold white]")
    console.print("[bold cyan]" + "="*80 + "[/bold cyan]")
    
    ids_processos = ["P1", "P2", "P3"]
    
    for pid in ids_processos:
        eventos_do_processo = [e for e in todos_os_eventos if e["processo_id"] == pid]
        eventos_ordenados_locais = sorted(eventos_do_processo, key=lambda item: item["timestamp"])
        
        if eventos_ordenados_locais:
            console.print(f"\n[bold underline]--- Histórico de {pid} ---[/bold underline]")
            for e in eventos_ordenados_locais:
                tipo = e['tipo']
                timestamp = e['timestamp']
                detalhes_originais = e['detalhes']
                
                if tipo == "SEND":
                    destino = extrair_participante(tipo, detalhes_originais)
                    console.print(f"{pid} [cyan]send[/cyan] para {destino} relógio lógico [bold magenta]{timestamp}[/bold magenta]")
                elif tipo == "RECEIVE":
                    origem = extrair_participante(tipo, detalhes_originais)
                    console.print(f"{pid} [green]receive[/green] de {origem} relógio lógico [bold magenta]{timestamp}[/bold magenta]")
                else: 
                    console.print(f"{pid} [yellow]exec[/yellow] relógio lógico [bold magenta]{timestamp}[/bold magenta]")

def exibir_resumo_final():
    """Calcula e exibe o relógio lógico final (máximo) de cada processo."""
    console.print("\n[bold white]ESTADO FINAL DOS RELÓGIOS LÓGICOS[/bold white]")
    
    paineis = []
    for pid in ["P1", "P2", "P3"]:
        # Filtra os eventos apenas deste processo e pega o maior timestamp
        eventos_pid = [e["timestamp"] for e in todos_os_eventos if e["processo_id"] == pid]
        relogio_final = max(eventos_pid) if eventos_pid else 0
        
        # Cria um mini painel para cada processo
        painel = Panel(
            f"[bold magenta]{relogio_final:^10}[/bold magenta]", 
            title=f"[bold cyan]{pid}[/bold cyan]", 
            expand=False,
            border_style="cyan"
        )
        paineis.append(painel)
        
    # Exibe os painéis lado a lado
    console.print(Columns(paineis))
    console.print()

def exibir_lista_unificada():
    tabela = Table(
        title="\nVISÃO GLOBAL: LISTA UNIFICADA (ORDENAÇÃO TOTAL DE LAMPORT)\nCritério: Menor Timestamp -> Desempate por ID do Processo",
        show_header=True, 
        header_style="bold white",
        title_style="bold magenta"
    )
    
    tabela.add_column("Ordem", style="dim", width=6, justify="center")
    tabela.add_column("Processo", justify="center", style="bold white")
    tabela.add_column("Evento", justify="center")
    tabela.add_column("Relógio Lógico", justify="center", style="bold magenta")
    tabela.add_column("Detalhes")

    eventos_ordenados = sorted(
        todos_os_eventos, 
        key=lambda item: (item["timestamp"], item["processo_id"])
    )
    
    for i, e in enumerate(eventos_ordenados, 1):
        cor_evento = "cyan" if e['tipo'] == "SEND" else "green" if e['tipo'] == "RECEIVE" else "yellow"
        
        tabela.add_row(
            str(i),
            e['processo_id'],
            f"[{cor_evento}]{e['tipo']}[/{cor_evento}]",
            str(e['timestamp']),
            e['detalhes']
        )
        
    console.print(tabela)
    console.print("[bold cyan]" + "="*80 + "[/bold cyan]\n")


async def roteador(websocket):
    try:
        id_processo = await websocket.recv()
    except websockets.exceptions.ConnectionClosed:
        return

    clientes_conectados[id_processo] = websocket
    console.print(f"[bold green][Servidor][/bold green] Processo {id_processo} conectado. ({len(clientes_conectados)}/{TOTAL_PROCESSOS})")

    if len(clientes_conectados) == TOTAL_PROCESSOS:
        console.print("[bold yellow][Servidor] Todos os processos conectados! Disparando sinal de INÍCIO...[/bold yellow]")
        for cliente_ws in clientes_conectados.values():
            await cliente_ws.send(json.dumps({"tipo_pacote": "INICIAR"}))

    try:
        async for mensagem in websocket:
            dados = json.loads(mensagem)
            tipo_pacote = dados.get("tipo_pacote")
            
            if tipo_pacote == "FINALIZADO":
                eventos_recebidos = dados.get("eventos", [])
                todos_os_eventos.extend(eventos_recebidos)
                processos_finalizados.add(id_processo)
                console.print(f"[bold green][Servidor][/bold green] Relatório final recebido de {id_processo}. ({len(processos_finalizados)}/{TOTAL_PROCESSOS})")
                
                if len(processos_finalizados) >= TOTAL_PROCESSOS:
                    exibir_listagem_por_processo()
                    
                    # Chama a nova função do estado final ANTES da tabela global
                    exibir_resumo_final()
                    
                    exibir_lista_unificada()
            
            else:
                destino = dados.get("destino")
                if destino in clientes_conectados:
                    await clientes_conectados[destino].send(json.dumps(dados))
                else:
                    console.print(f"[bold red][Servidor] Destino {destino} indisponível.[/bold red]")
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if id_processo in clientes_conectados:
            del clientes_conectados[id_processo]
        console.print(f"[dim][Servidor] Processo {id_processo} desconectado.[/dim]")

async def main():
    async with websockets.serve(roteador, "localhost", 8765):
        console.print("[bold cyan][Servidor] Rodando na porta 8765. Aguardando processos...[/bold cyan]")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())