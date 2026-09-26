O manual de execução fica 100% **dentro** do próprio arquivo `README.md`. Em projetos de software, o README e o manual de execução são exatamente o mesmo documento.

Copie todo o bloco de texto abaixo (usando o botão de copiar no canto da caixa) e cole diretamente dentro do seu arquivo `README.md` vazio. A seção 4 inteira é o seu passo a passo.

```markdown
# Trabalho Prático 2: Algoritmo de Relógios Lógicos de Lamport

Implementação do algoritmo de sincronização lógica de tempo de Lamport em Python com recurso a WebSockets (Abordagem A), com um Servidor Central roteador e nós autónomos (`P1`, `P2` e `P3`).

---

## 1. Requisitos de Ambiente

* **Linguagem / Runtime:** Python 3.8 ou superior.
* **Sistema Operativo:** Windows 10/11, Linux ou macOS.
* **Gestor de Pacotes:** `pip` instalado e atualizado.

---

## 2. Dependências e Instalação

Conteúdo do ficheiro `requirements.txt`:
websockets==11.0.3
rich==13.5.2

Comando de instalação no terminal:
pip install -r requirements.txt

---

## 3. Estrutura do Projeto

trabalho 2 sistemas distribuidos/
├── requirements.txt      # Dependências externas do projeto
├── server.py            # Servidor central roteador e agregador final
├── processo.py          # Nó distribuído com lógica de Lamport
└── README.md            # Este manual completo

---

## 4. Manual de Execução (Passo a Passo)

Abra **4 terminais** em paralelo (recomenda-se dividir os terminais integrados do VS Code lado a lado):

* **Passo 1: Iniciar o Servidor Central**
  No primeiro terminal, execute:
  python server.py
  *(O servidor iniciará na porta 8765 e aguardará a ligação dos 3 processos)*.

* **Passo 2: Iniciar o Processo P1**
  No segundo terminal, execute:
  python processo.py P1

* **Passo 3: Iniciar o Processo P2**
  No terceiro terminal, execute:
  python processo.py P2

* **Passo 4: Iniciar o Processo P3**
  No quarto terminal, execute:
  python processo.py P3

---

## 5. Dinâmica e Comportamento Operacional

1. **Barreira de Espera:** Os processos ligam-se ao servidor e ficam suspensos aguardando os restantes nós.
2. **Sinal de Início:** Quando o `P3` se conecta, o servidor envia a mensagem `INICIAR` em simultâneo para todos os terminais.
3. **Regras Lógicas de Lamport:**
   * **EXEC:** Incremento local ($L = L + 1$).
   * **SEND:** Incremento prévio ($L = L + 1$) com carimbo temporal no pacote.
   * **RECEIVE:** Atualização causal forçada: $L = \max(L_{\text{local}}, t_{\text{msg}}) + 1$.
4. **Relatório Final no Servidor:**
   * Apresentação da Visão Local por processo.
   * Apresentação dos painéis com o Estado Final dos relógios.
   * Apresentação da Visão Global (Tabela de Ordenação Total) com desempate determinístico por ID (`P1 < P2 < P3`).

```