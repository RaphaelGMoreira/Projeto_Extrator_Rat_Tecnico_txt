"""
Interface hibrida (GUI + CLI fallback)
Integrada com extrator.py

Responsabilidades deste arquivo:
- Selecionar um ou mais arquivos TXT (inclusive de pastas diferentes)
- Definir arquivo de saida Excel
- Aplicar filtro opcional por intervalo de datas
- Aplicar filtros avancados por tecnico/status/cidade e inconsistencias
- Acionar o processamento no extrator e mostrar tempo total
"""

import os
import re
import json
import queue
import shutil
import threading
import time
import traceback
import unicodedata
from datetime import datetime
import pandas as pd

# ==========================
# BLOCO 1: INTEGRACAO COM O EXTRATOR
# ==========================
# A interface chama `gerar_excel` do extrator principal.
# Se nao encontrar o modulo, usamos fallback para manter a aplicacao funcional.
try:
    from extrator import gerar_excel, recarregar_regras
except ImportError:
    def gerar_excel(arquivos, saida, data_inicio=None, data_fim=None, **kwargs):
        df = pd.DataFrame({"ARQUIVOS PROCESSADOS": arquivos})
        df.to_excel(saida, index=False)
        return {
            "registros_gerados": len(df),
            "registros_totais_lidos": len(df),
            "registros_unicos": len(df),
            "duplicados_removidos": 0,
            "registros_apos_filtros": len(df),
            "logs_gerados": 0,
            "arquivo_saida": saida,
        }

    def recarregar_regras():
        return {}

# ==========================
# BLOCO 2: DETECCAO DE AMBIENTE GRAFICO
# ==========================
# Quando Tkinter existe, a aplicacao abre no modo GUI.
# Sem Tkinter, usamos fluxo CLI no terminal.
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox
    from tkinter import ttk
    from tkinter import scrolledtext

    GUI_DISPONIVEL = True
except ModuleNotFoundError:
    GUI_DISPONIVEL = False


def _texto_chave(txt):
    base = unicodedata.normalize("NFD", str(txt or ""))
    base = base.encode("ascii", "ignore").decode()
    return " ".join(base.lower().strip().split())


def _normalizar_hhmm_interface(valor):
    txt = " ".join(str(valor or "").split()).replace(";", ":").replace(".", ":")
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", txt)
    if not m:
        return ""
    return f"{int(m.group(1)):02d}:{m.group(2)}"


def _formatar_duracao_hms(segundos):
    try:
        total = int(round(float(segundos)))
    except (TypeError, ValueError):
        total = 0
    if total < 0:
        total = 0
    horas = total // 3600
    minutos = (total % 3600) // 60
    segundos_rest = total % 60
    return f"{horas:02d}:{minutos:02d}:{segundos_rest:02d}"


def _ler_jsonl(path):
    itens = []
    if not os.path.isfile(path):
        return itens
    try:
        with open(path, encoding="utf-8") as f:
            for linha in f:
                txt = (linha or "").strip()
                if not txt:
                    continue
                try:
                    itens.append(json.loads(txt))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []
    return itens


def _usuario_local():
    for key in ("USERNAME", "USER", "LOGNAME"):
        v = os.environ.get(key)
        if v:
            return str(v)
    return "USUARIO_LOCAL"

# ==========================
# BLOCO 3: MODO GUI
# ==========================
if GUI_DISPONIVEL:

    class App:
        # Estado principal da interface (arquivos, filtros e status visual).
        def __init__(self, root):
            self.root = root
            self.root.title("Sistema de Extracao de RATs")
            self.root.geometry("920x700")
            self.root.minsize(860, 620)
            self.root.configure(bg="#f4f6f8")

            self.arquivos = []
            self.data_inicio_var = tk.StringVar()
            self.data_fim_var = tk.StringVar()
            self.filtro_tecnico_var = tk.StringVar()
            self.filtro_status_var = tk.StringVar()
            self.filtro_cidade_var = tk.StringVar()
            self.somente_incons_var = tk.BooleanVar(value=False)
            self.saida_var = tk.StringVar()
            self.tempo_processamento_var = tk.StringVar(value="Tempo de processamento: -")
            self.resumo_execucao_var = tk.StringVar(value="Resumo: aguardando seleção de arquivos.")
            self.resultado_resumo_var = tk.StringVar(value="Resultado: nenhuma execução ainda.")
            self.regras_tecnicos_sessao = []
            self.tecnicos_base = []
            self.indices_tecnicos_base_visiveis = []
            self.filtro_tecnicos_base_var = tk.StringVar()
            self.arquivo_tecnicos_base = os.path.join(
                os.path.dirname(__file__), "regras", "tecnicos_regras.json"
            )
            self.pasta_persistencia = os.path.join(os.path.dirname(__file__), "persistencia")
            self.arquivo_rats_desconhecidas = os.path.join(
                self.pasta_persistencia, "rats_desconhecidas.jsonl"
            )
            self.arquivo_decisoes_usuario = os.path.join(
                self.pasta_persistencia, "decisoes_usuario.jsonl"
            )
            self.blocos_desconhecidos = []
            self.indices_blocos_desconhecidos_visiveis = []
            self.filtro_blocos_desconhecidos_var = tk.StringVar()
            self.resumo_blocos_desconhecidos_var = tk.StringVar(
                value="BLOCOS DESCONHECIDOS: 0 | PENDENTES: 0"
            )
            self.avancado_aberto = True
            self.avancado_titulo_var = tk.StringVar(value="Campo Avancado [-]")
            self.resumo_tecnicos_var = tk.StringVar(value="Tecnicos na sessao: 0")
            self.resumo_tecnicos_base_var = tk.StringVar(value="Tecnicos da base: 0")
            self.frame_avancado = None
            self.lista_tecnicos_sessao = None
            self.lista_tecnicos_base = None
            self.lista_blocos_desconhecidos = None
            self.progress = None
            self.notebook = None
            self.log_text = None
            self.btn_abrir_excel = None
            self.btn_gerar = None
            self.execucao_em_andamento = False
            self.fila_execucao = queue.Queue()
            self.inicio_execucao = 0.0

            self.build_ui()
            self.atualizar_lista_tecnicos_sessao()
            self.carregar_tecnicos_base()
            self.carregar_blocos_desconhecidos()
            self.atualizar_resumo_execucao()

        def build_ui(self):
            # Estrutura visual principal: titulo e abas por contexto.
            titulo = tk.Label(
                self.root,
                text="Extrator de Scripts de RATs - Interface Grafica",
                font=("Segoe UI", 18, "bold"),
                bg="#f4f6f8",
            )
            titulo.pack(pady=12)

            frame_root = tk.Frame(self.root, bg="white", bd=1, relief="solid")
            frame_root.pack(padx=20, pady=(0, 12), fill="both", expand=True)

            self.notebook = ttk.Notebook(frame_root)
            self.notebook.pack(fill="both", expand=True, padx=8, pady=8)

            tab_processamento = tk.Frame(self.notebook, bg="white")
            tab_avancado = tk.Frame(self.notebook, bg="#f7f7f7")
            tab_resultado = tk.Frame(self.notebook, bg="white")

            self.notebook.add(tab_processamento, text="PROCESSAMENTO")
            self.notebook.add(tab_avancado, text="CAMPO AVANÇADO")
            self.notebook.add(tab_resultado, text="RESULTADO / LOG")

            # =====================
            # ABA 1: PROCESSAMENTO
            # =====================
            frame_arquivos = tk.Frame(tab_processamento, bg="white")
            frame_arquivos.pack(fill="x", padx=16, pady=(14, 8))

            ttk.Button(
                frame_arquivos,
                text="ADICIONAR ARQUIVOS TXT",
                command=self.selecionar_arquivos,
            ).pack(side="left")
            ttk.Button(
                frame_arquivos,
                text="ADICIONAR PASTA",
                command=self.selecionar_pasta,
            ).pack(side="left", padx=8)
            ttk.Button(
                frame_arquivos,
                text="REMOVER SELECIONADO",
                command=self.remover_arquivo_selecionado,
            ).pack(side="left")
            ttk.Button(
                frame_arquivos,
                text="LIMPAR LISTA",
                command=self.limpar_lista_arquivos,
            ).pack(side="left", padx=8)

            self.lista = tk.Listbox(tab_processamento, height=10, selectmode=tk.EXTENDED)
            self.lista.pack(fill="both", padx=16, pady=(0, 10), expand=True)

            frame_saida = tk.Frame(tab_processamento, bg="white")
            frame_saida.pack(fill="x", padx=16, pady=(0, 10))

            ttk.Entry(frame_saida, textvariable=self.saida_var).pack(side="left", fill="x", expand=True)
            ttk.Button(frame_saida, text="SALVAR COMO", command=self.selecionar_saida).pack(side="left", padx=6)
            self.btn_pasta = ttk.Button(
                frame_saida,
                text="PASTA DO ARQUIVO",
                command=self.abrir_pasta_saida,
                state="disabled",
            )
            self.btn_pasta.pack(side="left", padx=4)

            frame_periodo = tk.Frame(tab_processamento, bg="white")
            frame_periodo.pack(fill="x", padx=16, pady=(0, 4))

            tk.Label(frame_periodo, text="DATA INICIAL (DD/MM/AAAA):", bg="white").grid(
                row=0, column=0, sticky="w", padx=(0, 8)
            )
            ttk.Entry(frame_periodo, textvariable=self.data_inicio_var, width=18).grid(
                row=0, column=1, sticky="w"
            )
            tk.Label(frame_periodo, text="DATA FINAL (DD/MM/AAAA):", bg="white").grid(
                row=0, column=2, sticky="w", padx=(18, 8)
            )
            ttk.Entry(frame_periodo, textvariable=self.data_fim_var, width=18).grid(
                row=0, column=3, sticky="w"
            )

            frame_filtros = tk.Frame(tab_processamento, bg="white")
            frame_filtros.pack(fill="x", padx=16, pady=(0, 6))

            tk.Label(frame_filtros, text="FILTRO TÉCNICO:", bg="white").grid(
                row=0, column=0, sticky="w", padx=(0, 8)
            )
            ttk.Entry(frame_filtros, textvariable=self.filtro_tecnico_var, width=20).grid(
                row=0, column=1, sticky="w"
            )
            tk.Label(frame_filtros, text="FILTRO STATUS:", bg="white").grid(
                row=0, column=2, sticky="w", padx=(18, 8)
            )
            ttk.Entry(frame_filtros, textvariable=self.filtro_status_var, width=18).grid(
                row=0, column=3, sticky="w"
            )
            tk.Label(frame_filtros, text="FILTRO CIDADE:", bg="white").grid(
                row=1, column=0, sticky="w", padx=(0, 8), pady=(6, 0)
            )
            ttk.Entry(frame_filtros, textvariable=self.filtro_cidade_var, width=20).grid(
                row=1, column=1, sticky="w", pady=(6, 0)
            )
            ttk.Checkbutton(
                frame_filtros,
                text="SOMENTE INCONSISTÊNCIAS",
                variable=self.somente_incons_var,
                command=self.atualizar_resumo_execucao,
            ).grid(row=1, column=2, columnspan=2, sticky="w", padx=(18, 8), pady=(6, 0))

            frame_resumo = tk.Frame(tab_processamento, bg="#eef3ff", bd=1, relief="solid")
            frame_resumo.pack(fill="x", padx=16, pady=(4, 8))
            tk.Label(
                frame_resumo,
                text="RESUMO PRÉ-PROCESSAMENTO",
                bg="#eef3ff",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=8, pady=(6, 2))
            tk.Label(
                frame_resumo,
                textvariable=self.resumo_execucao_var,
                bg="#eef3ff",
                justify="left",
                anchor="w",
                wraplength=900,
            ).pack(fill="x", padx=8, pady=(0, 8))

            self.progress = ttk.Progressbar(
                tab_processamento,
                orient="horizontal",
                length=420,
                mode="indeterminate",
            )
            self.progress.pack(pady=(0, 8))

            tk.Label(tab_processamento, textvariable=self.tempo_processamento_var, bg="white").pack(pady=(0, 8))

            self.btn_gerar = tk.Button(
                tab_processamento,
                text="GERAR EXCEL",
                bg="#28a745",
                fg="white",
                font=("Segoe UI", 10, "bold"),
                command=self.executar,
            )
            self.btn_gerar.pack(pady=(0, 14))

            # =====================
            # ABA 2: CAMPO AVANÇADO
            # =====================
            self.frame_avancado = tk.Frame(tab_avancado, bg="#f7f7f7")
            self.frame_avancado.pack(fill="both", expand=True, padx=12, pady=12)

            tk.Label(
                self.frame_avancado,
                text="CADASTRO TEMPORÁRIO DE TÉCNICOS (APLICA SOMENTE NESTA EXECUÇÃO).",
                bg="#f7f7f7",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=10, pady=(8, 4))

            self.lista_tecnicos_sessao = tk.Listbox(self.frame_avancado, height=6, selectmode=tk.SINGLE)
            self.lista_tecnicos_sessao.pack(fill="x", padx=10, pady=4)

            frame_botoes_tecnicos = tk.Frame(self.frame_avancado, bg="#f7f7f7")
            frame_botoes_tecnicos.pack(fill="x", padx=10, pady=(2, 6))
            ttk.Button(
                frame_botoes_tecnicos,
                text="ADICIONAR TÉCNICO",
                command=self.adicionar_tecnico_sessao,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_tecnicos,
                text="EDITAR TÉCNICO",
                command=self.editar_tecnico_sessao,
            ).pack(side="left", padx=6)
            ttk.Button(
                frame_botoes_tecnicos,
                text="EXCLUIR SELECIONADO",
                command=self.excluir_tecnico_sessao,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_tecnicos,
                text="LIMPAR TÉCNICOS",
                command=self.limpar_tecnicos_sessao,
            ).pack(side="left", padx=6)

            tk.Label(
                self.frame_avancado,
                textvariable=self.resumo_tecnicos_var,
                bg="#f7f7f7",
            ).pack(anchor="w", padx=10, pady=(0, 8))

            ttk.Separator(self.frame_avancado, orient="horizontal").pack(fill="x", padx=10, pady=(0, 8))

            tk.Label(
                self.frame_avancado,
                text="CADASTRO PERSISTENTE DE TÉCNICOS (SALVO NA BASE).",
                bg="#f7f7f7",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=10, pady=(0, 4))

            frame_filtro_base = tk.Frame(self.frame_avancado, bg="#f7f7f7")
            frame_filtro_base.pack(fill="x", padx=10, pady=(0, 2))
            tk.Label(frame_filtro_base, text="BUSCAR TÉCNICO BASE:", bg="#f7f7f7").pack(side="left")
            ent_filtro_base = ttk.Entry(
                frame_filtro_base,
                textvariable=self.filtro_tecnicos_base_var,
                width=36,
            )
            ent_filtro_base.pack(side="left", padx=6)
            ent_filtro_base.bind("<KeyRelease>", lambda _e: self.atualizar_lista_tecnicos_base())
            ttk.Button(
                frame_filtro_base,
                text="LIMPAR BUSCA",
                command=self.limpar_filtro_tecnico_base,
            ).pack(side="left")

            self.lista_tecnicos_base = tk.Listbox(self.frame_avancado, height=6, selectmode=tk.SINGLE)
            self.lista_tecnicos_base.pack(fill="x", padx=10, pady=4)

            frame_botoes_base = tk.Frame(self.frame_avancado, bg="#f7f7f7")
            frame_botoes_base.pack(fill="x", padx=10, pady=(2, 6))

            ttk.Button(
                frame_botoes_base,
                text="ADICIONAR NA BASE",
                command=self.adicionar_tecnico_base,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_base,
                text="EDITAR NA BASE",
                command=self.editar_tecnico_base,
            ).pack(side="left", padx=6)
            ttk.Button(
                frame_botoes_base,
                text="OCULTAR TÉCNICO",
                command=self.ocultar_tecnico_base,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_base,
                text="LISTAR OCULTOS",
                command=self.listar_tecnicos_ocultos,
            ).pack(side="left", padx=6)
            ttk.Button(
                frame_botoes_base,
                text="ATUALIZAR LISTA",
                command=self.carregar_tecnicos_base,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_base,
                text="RECARREGAR REGRAS",
                command=self.recarregar_regras_externas,
            ).pack(side="right")

            tk.Label(
                self.frame_avancado,
                textvariable=self.resumo_tecnicos_base_var,
                bg="#f7f7f7",
            ).pack(anchor="w", padx=10, pady=(0, 8))

            ttk.Separator(self.frame_avancado, orient="horizontal").pack(fill="x", padx=10, pady=(0, 8))

            tk.Label(
                self.frame_avancado,
                text="REVISÃO DE BLOCOS DESCONHECIDOS (MODO APRENDIZADO).",
                bg="#f7f7f7",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=10, pady=(0, 4))

            frame_filtro_desconhecidos = tk.Frame(self.frame_avancado, bg="#f7f7f7")
            frame_filtro_desconhecidos.pack(fill="x", padx=10, pady=(0, 2))
            tk.Label(
                frame_filtro_desconhecidos,
                text="BUSCAR BLOCO:",
                bg="#f7f7f7",
            ).pack(side="left")
            ent_filtro_desconhecidos = ttk.Entry(
                frame_filtro_desconhecidos,
                textvariable=self.filtro_blocos_desconhecidos_var,
                width=36,
            )
            ent_filtro_desconhecidos.pack(side="left", padx=6)
            ent_filtro_desconhecidos.bind("<KeyRelease>", lambda _e: self.atualizar_lista_blocos_desconhecidos())
            ttk.Button(
                frame_filtro_desconhecidos,
                text="LIMPAR BUSCA",
                command=self.limpar_filtro_blocos_desconhecidos,
            ).pack(side="left")

            self.lista_blocos_desconhecidos = tk.Listbox(
                self.frame_avancado,
                height=7,
                selectmode=tk.SINGLE,
            )
            self.lista_blocos_desconhecidos.pack(fill="x", padx=10, pady=4)

            frame_botoes_desconhecidos = tk.Frame(self.frame_avancado, bg="#f7f7f7")
            frame_botoes_desconhecidos.pack(fill="x", padx=10, pady=(2, 6))
            ttk.Button(
                frame_botoes_desconhecidos,
                text="ATUALIZAR BLOCO",
                command=self.carregar_blocos_desconhecidos,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_desconhecidos,
                text="VER DETALHES",
                command=self.ver_bloco_desconhecido,
            ).pack(side="left", padx=6)
            ttk.Button(
                frame_botoes_desconhecidos,
                text="AJUSTAR E CONFIRMAR",
                command=self.ajustar_confirmar_bloco_desconhecido,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_desconhecidos,
                text="CONFIRMAR VÁLIDO",
                command=self.confirmar_bloco_desconhecido,
            ).pack(side="left", padx=6)
            ttk.Button(
                frame_botoes_desconhecidos,
                text="IGNORAR BLOCO",
                command=self.ignorar_bloco_desconhecido,
            ).pack(side="left")
            ttk.Button(
                frame_botoes_desconhecidos,
                text="VOLTAR PARA PENDENTE",
                command=self.reabrir_bloco_desconhecido,
            ).pack(side="left")

            tk.Label(
                self.frame_avancado,
                textvariable=self.resumo_blocos_desconhecidos_var,
                bg="#f7f7f7",
            ).pack(anchor="w", padx=10, pady=(0, 8))

            # =====================
            # ABA 3: RESULTADO / LOG
            # =====================
            frame_resultado = tk.Frame(tab_resultado, bg="white")
            frame_resultado.pack(fill="x", padx=16, pady=(14, 8))

            tk.Label(
                frame_resultado,
                text="PAINEL DA ÚLTIMA EXECUÇÃO",
                bg="white",
                font=("Segoe UI", 10, "bold"),
            ).pack(anchor="w")
            tk.Label(
                frame_resultado,
                textvariable=self.resultado_resumo_var,
                bg="white",
                justify="left",
                anchor="w",
                wraplength=900,
            ).pack(fill="x", pady=(4, 8))

            frame_btn_resultado = tk.Frame(frame_resultado, bg="white")
            frame_btn_resultado.pack(fill="x")

            self.btn_abrir_excel = ttk.Button(
                frame_btn_resultado,
                text="ABRIR EXCEL",
                command=self.abrir_arquivo_saida,
                state="disabled",
            )
            self.btn_abrir_excel.pack(side="left")
            ttk.Button(
                frame_btn_resultado,
                text="ABRIR PASTA DO ARQUIVO",
                command=self.abrir_pasta_saida,
            ).pack(side="left", padx=6)

            tk.Label(
                tab_resultado,
                text="LOG DA INTERFACE",
                bg="white",
                font=("Segoe UI", 10, "bold"),
            ).pack(anchor="w", padx=16, pady=(4, 2))

            self.log_text = scrolledtext.ScrolledText(tab_resultado, height=16, wrap="word")
            self.log_text.pack(fill="both", expand=True, padx=16, pady=(0, 16))
            self.log_text.configure(state="disabled")

            # Atualizações automáticas do resumo pré-processamento.
            for var in (
                self.data_inicio_var,
                self.data_fim_var,
                self.filtro_tecnico_var,
                self.filtro_status_var,
                self.filtro_cidade_var,
                self.saida_var,
                self.somente_incons_var,
            ):
                var.trace_add("write", lambda *_args: self.atualizar_resumo_execucao())
            self.atualizar_botao_pasta()

        def registrar_log_interface(self, mensagem):
            if self.log_text is None:
                return
            stamp = datetime.now().strftime("%H:%M:%S")
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, f"[{stamp}] {mensagem}\n")
            self.log_text.see(tk.END)
            self.log_text.configure(state="disabled")

        def atualizar_resumo_execucao(self):
            qtd_arquivos = len(self.arquivos)
            data_ini = self.data_inicio_var.get().strip() or "-"
            data_fim = self.data_fim_var.get().strip() or "-"
            filtro_tecnico = self.filtro_tecnico_var.get().strip() or "-"
            filtro_status = self.filtro_status_var.get().strip() or "-"
            filtro_cidade = self.filtro_cidade_var.get().strip() or "-"
            inconsistencias = "SIM" if self.somente_incons_var.get() else "NÃO"
            saida = self.saida_var.get().strip() or "(não definido)"
            resumo = (
                f"Arquivos: {qtd_arquivos} | Período: {data_ini} até {data_fim} | "
                f"Técnicos temporários: {len(self.regras_tecnicos_sessao)} | "
                f"Técnicos base visíveis: {len(self.indices_tecnicos_base_visiveis)} | "
                f"Filtro técnico: {filtro_tecnico} | Filtro status: {filtro_status} | "
                f"Filtro cidade: {filtro_cidade} | Somente inconsistências: {inconsistencias} | "
                f"Saída: {saida}"
            )
            self.resumo_execucao_var.set(resumo)

        def atualizar_painel_resultado(self, resumo, duracao):
            resumo = resumo or {}
            duracao_txt = _formatar_duracao_hms(duracao)
            texto = (
                f"Tempo: {duracao_txt}\n"
                f"Registros gerados: {resumo.get('registros_gerados', '-')}\n"
                f"Registros lidos: {resumo.get('registros_totais_lidos', '-')}\n"
                f"Registros únicos: {resumo.get('registros_unicos', '-')}\n"
                f"Duplicados removidos: {resumo.get('duplicados_removidos', '-')}\n"
                f"Registros após filtros: {resumo.get('registros_apos_filtros', '-')}\n"
                f"Logs gerados: {resumo.get('logs_gerados', '-')}\n"
                f"Blocos desconhecidos: {resumo.get('blocos_desconhecidos_persistidos', '-')}\n"
                f"Arquivo de saída: {resumo.get('arquivo_saida', self.saida_var.get().strip())}"
            )
            self.resultado_resumo_var.set(texto)
            if self.notebook is not None:
                self.notebook.select(2)

        def abrir_arquivo_saida(self):
            caminho_saida = self.saida_var.get().strip()
            if not caminho_saida:
                messagebox.showerror("Erro", "Defina o arquivo de saída primeiro.")
                return
            if not os.path.isfile(caminho_saida):
                messagebox.showerror("Erro", "Arquivo de saída não encontrado.")
                return
            os.startfile(caminho_saida)

        def selecionar_arquivos(self):
            # Permite adicionar arquivos de qualquer pasta em selecoes sucessivas.
            arquivos = filedialog.askopenfilenames(filetypes=[("TXT", "*.txt")])
            if not arquivos:
                return
            self.adicionar_arquivos(arquivos)

        def alternar_campo_avancado(self):
            # Mantido por compatibilidade: agora o campo avançado é uma aba dedicada.
            self.avancado_aberto = True
            self.avancado_titulo_var.set("Campo Avancado [-]")
            if self.notebook is not None:
                self.notebook.select(1)

        def recarregar_regras_externas(self):
            try:
                resumo = recarregar_regras() or {}
                self.carregar_tecnicos_base()
                self.atualizar_resumo_execucao()
                tecnicos = resumo.get("tecnicos_regras", 0)
                categorias = resumo.get("categoria_ordem", 0)
                palavras = resumo.get("categorias_palavras", 0)
                self.registrar_log_interface("Regras externas recarregadas com sucesso.")
                messagebox.showinfo(
                    "Regras atualizadas",
                    (
                        "Regras recarregadas com sucesso.\n"
                        f"Tecnicos: {tecnicos}\n"
                        f"Categorias: {categorias}\n"
                        f"Palavras-chave: {palavras}"
                    ),
                )
            except Exception as e:
                self.registrar_log_interface(f"Falha ao recarregar regras: {e}")
                messagebox.showerror("Erro", f"Falha ao recarregar regras:\n{e}")

        def atualizar_lista_tecnicos_sessao(self):
            if self.lista_tecnicos_sessao is None:
                return
            self.lista_tecnicos_sessao.delete(0, tk.END)
            for idx, regra in enumerate(self.regras_tecnicos_sessao, start=1):
                linha = self._linha_resumo_tecnico(idx, regra)
                self.lista_tecnicos_sessao.insert(tk.END, linha)
            self.resumo_tecnicos_var.set(f"Tecnicos na sessao: {len(self.regras_tecnicos_sessao)}")
            self.atualizar_resumo_execucao()

        def _validar_dados_tecnico(
            self,
            match,
            estado,
            cidade,
            endereco_partida,
            tecnico_saida,
            categoria_fixa,
            horario_inicio_expediente,
            horario_fim_expediente,
            regras_existentes=None,
            indice_edicao=None,
            verificar_duplicidade=True,
        ):
            erros = []

            if not match:
                erros.append("TECNICO ENTRADA é obrigatório.")
            elif len(_texto_chave(match)) < 2:
                erros.append("TECNICO ENTRADA deve ter pelo menos 2 caracteres.")

            if not estado:
                erros.append("ESTADO UF é obrigatório.")
            elif not re.fullmatch(r"[A-Z]{2}", estado):
                erros.append("ESTADO UF deve conter exatamente 2 letras (ex.: SP).")

            if not cidade:
                erros.append("CIDADE é obrigatória.")
            elif len(cidade) < 2:
                erros.append("CIDADE deve ter pelo menos 2 caracteres.")

            if not endereco_partida:
                erros.append("ENDERECO DE PARTIDA é obrigatório.")
            elif len(endereco_partida) < 5:
                erros.append("ENDERECO DE PARTIDA deve ter pelo menos 5 caracteres.")

            if tecnico_saida and len(tecnico_saida) < 3:
                erros.append("TECNICO SAIDA, quando informado, deve ter pelo menos 3 caracteres.")

            if categoria_fixa and len(categoria_fixa) < 3:
                erros.append("CATEGORIA FIXA, quando informada, deve ter pelo menos 3 caracteres.")

            if not horario_inicio_expediente:
                erros.append("HORARIO INICIO DO EXPEDIENTE é obrigatório (HH:mm).")
            elif not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", horario_inicio_expediente):
                erros.append("HORARIO INICIO DO EXPEDIENTE inválido. Use HH:mm.")

            if not horario_fim_expediente:
                erros.append("HORARIO FIM DO EXPEDIENTE é obrigatório (HH:mm).")
            elif not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", horario_fim_expediente):
                erros.append("HORARIO FIM DO EXPEDIENTE inválido. Use HH:mm.")

            if verificar_duplicidade:
                chave_match = _texto_chave(match)
                base_regras = (
                    self.regras_tecnicos_sessao
                    if regras_existentes is None
                    else list(regras_existentes)
                )
                for idx_existente, regra_existente in enumerate(base_regras):
                    if indice_edicao is not None and idx_existente == indice_edicao:
                        continue
                    if _texto_chave(regra_existente.get("match", "")) == chave_match:
                        erros.append("Ja existe uma regra com este TECNICO ENTRADA.")
                        break

            return erros

        def _abrir_modal_tecnico(self, indice=None):
            editando = indice is not None
            atual = self.regras_tecnicos_sessao[indice] if editando else {}

            modal = tk.Toplevel(self.root)
            modal.title("Editar tecnico" if editando else "Adicionar tecnico")
            modal.geometry("760x420")
            modal.transient(self.root)
            modal.grab_set()

            campos = [
                ("match", "TECNICO ENTRADA (OBRIGATORIO):"),
                ("tecnico_saida", "TECNICO SAIDA (OPCIONAL):"),
                ("estado", "ESTADO UF (OBRIGATORIO):"),
                ("cidade", "CIDADE (OBRIGATORIO):"),
                ("endereco_partida", "ENDERECO DE PARTIDA (OBRIGATORIO):"),
                ("categoria_fixa", "CATEGORIA FIXA (OPCIONAL):"),
                ("horario_inicio_expediente", "HORARIO INICIO DO EXPEDIENTE (OBRIGATORIO - HH:mm):"),
                ("horario_fim_expediente", "HORARIO FIM DO EXPEDIENTE (OBRIGATORIO - HH:mm):"),
            ]
            vars_form = {k: tk.StringVar(value=atual.get(k, "")) for k, _ in campos}

            frame_form = tk.Frame(modal, padx=12, pady=10)
            frame_form.pack(fill="both", expand=True)

            for row, (chave, rotulo) in enumerate(campos):
                tk.Label(frame_form, text=rotulo).grid(row=row, column=0, sticky="w", pady=4)
                ttk.Entry(frame_form, textvariable=vars_form[chave], width=70).grid(
                    row=row,
                    column=1,
                    sticky="we",
                    pady=4,
                    padx=(8, 0),
                )

            frame_form.columnconfigure(1, weight=1)

            def salvar():
                match = " ".join(vars_form["match"].get().split()).strip()
                tecnico_saida = " ".join(vars_form["tecnico_saida"].get().split()).strip()
                estado = vars_form["estado"].get().strip().upper()
                cidade = " ".join(vars_form["cidade"].get().split()).strip()
                endereco_partida = " ".join(vars_form["endereco_partida"].get().split()).strip()
                categoria_fixa = vars_form["categoria_fixa"].get().strip().upper()
                horario_inicio_expediente = _normalizar_hhmm_interface(
                    vars_form["horario_inicio_expediente"].get()
                )
                horario_fim_expediente = _normalizar_hhmm_interface(
                    vars_form["horario_fim_expediente"].get()
                )

                erros_validacao = self._validar_dados_tecnico(
                    match=match,
                    estado=estado,
                    cidade=cidade,
                    endereco_partida=endereco_partida,
                    tecnico_saida=tecnico_saida,
                    categoria_fixa=categoria_fixa,
                    horario_inicio_expediente=horario_inicio_expediente,
                    horario_fim_expediente=horario_fim_expediente,
                    regras_existentes=self.regras_tecnicos_sessao,
                    indice_edicao=indice if editando else None,
                )
                if erros_validacao:
                    messagebox.showerror("Erro", "\n".join(erros_validacao), parent=modal)
                    return

                novo = {
                    "match": match,
                    "tecnico_saida": tecnico_saida,
                    "estado": estado,
                    "cidade": cidade,
                    "endereco_partida": endereco_partida,
                    "categoria_fixa": categoria_fixa,
                    "horario_inicio_expediente": horario_inicio_expediente,
                    "horario_fim_expediente": horario_fim_expediente,
                }
                if editando:
                    self.regras_tecnicos_sessao[indice] = novo
                else:
                    self.regras_tecnicos_sessao.append(novo)
                self.atualizar_lista_tecnicos_sessao()
                modal.destroy()

            frame_btn = tk.Frame(modal, padx=12, pady=10)
            frame_btn.pack(fill="x")
            ttk.Button(frame_btn, text="Salvar", command=salvar).pack(side="left")
            ttk.Button(frame_btn, text="Cancelar", command=modal.destroy).pack(side="left", padx=8)

        def adicionar_tecnico_sessao(self):
            self._abrir_modal_tecnico(indice=None)

        def editar_tecnico_sessao(self):
            if self.lista_tecnicos_sessao is None:
                return
            selecionado = self.lista_tecnicos_sessao.curselection()
            if not selecionado:
                messagebox.showerror("Erro", "Selecione um tecnico para editar.")
                return
            self._abrir_modal_tecnico(indice=int(selecionado[0]))

        def excluir_tecnico_sessao(self):
            if self.lista_tecnicos_sessao is None:
                return
            selecionado = self.lista_tecnicos_sessao.curselection()
            if not selecionado:
                messagebox.showerror("Erro", "Selecione um tecnico para excluir.")
                return
            indice = int(selecionado[0])
            del self.regras_tecnicos_sessao[indice]
            self.atualizar_lista_tecnicos_sessao()

        def limpar_tecnicos_sessao(self):
            if not self.regras_tecnicos_sessao:
                return
            confirmar = messagebox.askyesno(
                "Confirmacao",
                "Deseja remover todos os tecnicos cadastrados na sessao?",
            )
            if not confirmar:
                return
            self.regras_tecnicos_sessao = []
            self.atualizar_lista_tecnicos_sessao()

        def _bool_ativo(self, valor):
            if isinstance(valor, bool):
                return valor
            texto = _texto_chave(valor)
            if texto in {"0", "false", "falso", "nao", "n", "off"}:
                return False
            return True

        def _normalizar_regra_base(self, regra):
            base = dict(regra) if isinstance(regra, dict) else {}
            base["match"] = " ".join(str(base.get("match", "")).split()).strip()
            base["tecnico_saida"] = " ".join(str(base.get("tecnico_saida", "")).split()).strip()
            base["estado"] = str(base.get("estado", "")).strip().upper()
            base["cidade"] = " ".join(str(base.get("cidade", "")).split()).strip()
            base["endereco_partida"] = " ".join(str(base.get("endereco_partida", "")).split()).strip()
            base["categoria_fixa"] = str(base.get("categoria_fixa", "")).strip().upper()
            base["horario_inicio_expediente"] = _normalizar_hhmm_interface(
                base.get("horario_inicio_expediente", "")
            )
            base["horario_fim_expediente"] = _normalizar_hhmm_interface(
                base.get("horario_fim_expediente", "")
            )
            base["ativo"] = self._bool_ativo(base.get("ativo", True))
            return base

        def _ler_json_arquivo(self, caminho):
            ultimo_erro = None
            for enc in ("utf-8-sig", "utf-8"):
                try:
                    with open(caminho, encoding=enc) as f:
                        return json.load(f)
                except Exception as e:
                    ultimo_erro = e
            if ultimo_erro:
                raise ultimo_erro
            return None

        def _criar_backup_base(self):
            caminho = self.arquivo_tecnicos_base
            if not os.path.exists(caminho):
                return ""
            pasta_backup = os.path.join(os.path.dirname(caminho), "backup")
            os.makedirs(pasta_backup, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            destino = os.path.join(pasta_backup, f"tecnicos_regras_{stamp}.json")
            shutil.copy2(caminho, destino)
            return destino

        def _salvar_tecnicos_base(self):
            caminho = self.arquivo_tecnicos_base
            os.makedirs(os.path.dirname(caminho), exist_ok=True)
            backup = self._criar_backup_base()
            conteudo = [self._normalizar_regra_base(r) for r in self.tecnicos_base if isinstance(r, dict)]
            with open(caminho, "w", encoding="utf-8") as f:
                json.dump(conteudo, f, ensure_ascii=False, indent=2)
            self.tecnicos_base = conteudo
            recarregar_regras()
            return backup

        def _linha_resumo_tecnico(self, idx, regra):
            match = regra.get("match", "") or "(SEM MATCH)"
            tecnico_saida = regra.get("tecnico_saida", "") or "(SEM TECNICO_SAIDA)"
            estado = regra.get("estado", "") or "--"
            cidade = regra.get("cidade", "") or "(SEM CIDADE)"
            categoria_fixa = regra.get("categoria_fixa", "")
            inicio_exp = regra.get("horario_inicio_expediente", "") or "--:--"
            fim_exp = regra.get("horario_fim_expediente", "") or "--:--"
            cat_txt = f" | CAT: {categoria_fixa}" if categoria_fixa else ""
            return f"{idx}. {match} -> {tecnico_saida} | {estado}/{cidade} | EXP: {inicio_exp}-{fim_exp}{cat_txt}"

        def limpar_filtro_tecnico_base(self):
            self.filtro_tecnicos_base_var.set("")
            self.atualizar_lista_tecnicos_base()

        def atualizar_lista_tecnicos_base(self):
            if self.lista_tecnicos_base is None:
                return

            self.lista_tecnicos_base.delete(0, tk.END)
            self.indices_tecnicos_base_visiveis = []
            filtro = _texto_chave(self.filtro_tecnicos_base_var.get())

            visiveis_total = 0
            visiveis_filtrados = 0
            ocultos_total = 0
            for idx_real, regra in enumerate(self.tecnicos_base):
                if self._bool_ativo(regra.get("ativo", True)):
                    visiveis_total += 1
                    alvo_filtro = _texto_chave(
                        " ".join(
                            [
                                regra.get("match", ""),
                                regra.get("tecnico_saida", ""),
                                regra.get("estado", ""),
                                regra.get("cidade", ""),
                                regra.get("categoria_fixa", ""),
                            ]
                        )
                    )
                    if filtro and filtro not in alvo_filtro:
                        continue
                    visiveis_filtrados += 1
                    self.indices_tecnicos_base_visiveis.append(idx_real)
                    self.lista_tecnicos_base.insert(
                        tk.END,
                        self._linha_resumo_tecnico(visiveis_filtrados, regra),
                    )
                else:
                    ocultos_total += 1

            total = len(self.tecnicos_base)
            if filtro:
                self.resumo_tecnicos_base_var.set(
                    (
                        f"Tecnicos da base: {visiveis_filtrados} visiveis (filtrado) | "
                        f"{visiveis_total} visiveis total | {ocultos_total} ocultos | total {total}"
                    )
                )
            else:
                self.resumo_tecnicos_base_var.set(
                    f"Tecnicos da base: {visiveis_total} visiveis | {ocultos_total} ocultos | total {total}"
                )

        def carregar_tecnicos_base(self):
            try:
                if not os.path.exists(self.arquivo_tecnicos_base):
                    self.tecnicos_base = []
                else:
                    dados = self._ler_json_arquivo(self.arquivo_tecnicos_base)
                    if not isinstance(dados, list):
                        raise ValueError("Arquivo de tecnicos da base deve ser uma lista JSON.")
                    self.tecnicos_base = [self._normalizar_regra_base(r) for r in dados if isinstance(r, dict)]
                self.atualizar_lista_tecnicos_base()
                self.atualizar_resumo_execucao()
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao carregar tecnicos da base:\n{e}")

        def _indice_real_base_selecionado(self):
            if self.lista_tecnicos_base is None:
                return None
            selecionado = self.lista_tecnicos_base.curselection()
            if not selecionado:
                return None
            idx_visivel = int(selecionado[0])
            if idx_visivel < 0 or idx_visivel >= len(self.indices_tecnicos_base_visiveis):
                return None
            return self.indices_tecnicos_base_visiveis[idx_visivel]

        def _buscar_duplicado_base(self, match, ignorar_indice=None):
            chave = _texto_chave(match)
            for idx, regra in enumerate(self.tecnicos_base):
                if ignorar_indice is not None and idx == ignorar_indice:
                    continue
                if _texto_chave(regra.get("match", "")) == chave:
                    return idx
            return None

        def _abrir_modal_tecnico_base(self, indice_real=None):
            editando = indice_real is not None
            atual = (
                self.tecnicos_base[indice_real]
                if editando and 0 <= indice_real < len(self.tecnicos_base)
                else {}
            )

            modal = tk.Toplevel(self.root)
            modal.title("Editar tecnico da base" if editando else "Adicionar tecnico na base")
            modal.geometry("780x430")
            modal.transient(self.root)
            modal.grab_set()

            campos = [
                ("match", "TECNICO ENTRADA (OBRIGATORIO):"),
                ("tecnico_saida", "TECNICO SAIDA (OPCIONAL):"),
                ("estado", "ESTADO UF (OBRIGATORIO):"),
                ("cidade", "CIDADE (OBRIGATORIO):"),
                ("endereco_partida", "ENDERECO DE PARTIDA (OBRIGATORIO):"),
                ("categoria_fixa", "CATEGORIA FIXA (OPCIONAL):"),
                ("horario_inicio_expediente", "HORARIO INICIO DO EXPEDIENTE (OBRIGATORIO - HH:mm):"),
                ("horario_fim_expediente", "HORARIO FIM DO EXPEDIENTE (OBRIGATORIO - HH:mm):"),
            ]
            vars_form = {k: tk.StringVar(value=atual.get(k, "")) for k, _ in campos}

            frame_form = tk.Frame(modal, padx=12, pady=10)
            frame_form.pack(fill="both", expand=True)

            for row, (chave, rotulo) in enumerate(campos):
                tk.Label(frame_form, text=rotulo).grid(row=row, column=0, sticky="w", pady=4)
                ttk.Entry(frame_form, textvariable=vars_form[chave], width=75).grid(
                    row=row,
                    column=1,
                    sticky="we",
                    pady=4,
                    padx=(8, 0),
                )

            frame_form.columnconfigure(1, weight=1)

            def salvar():
                match = " ".join(vars_form["match"].get().split()).strip()
                tecnico_saida = " ".join(vars_form["tecnico_saida"].get().split()).strip()
                estado = vars_form["estado"].get().strip().upper()
                cidade = " ".join(vars_form["cidade"].get().split()).strip()
                endereco_partida = " ".join(vars_form["endereco_partida"].get().split()).strip()
                categoria_fixa = vars_form["categoria_fixa"].get().strip().upper()
                horario_inicio_expediente = _normalizar_hhmm_interface(
                    vars_form["horario_inicio_expediente"].get()
                )
                horario_fim_expediente = _normalizar_hhmm_interface(
                    vars_form["horario_fim_expediente"].get()
                )

                erros_validacao = self._validar_dados_tecnico(
                    match=match,
                    estado=estado,
                    cidade=cidade,
                    endereco_partida=endereco_partida,
                    tecnico_saida=tecnico_saida,
                    categoria_fixa=categoria_fixa,
                    horario_inicio_expediente=horario_inicio_expediente,
                    horario_fim_expediente=horario_fim_expediente,
                    regras_existentes=self.tecnicos_base,
                    indice_edicao=indice_real if editando else None,
                    verificar_duplicidade=False,
                )
                if erros_validacao:
                    messagebox.showerror("Erro", "\n".join(erros_validacao), parent=modal)
                    return

                duplicado_idx = self._buscar_duplicado_base(
                    match,
                    ignorar_indice=indice_real if editando else None,
                )
                if duplicado_idx is not None:
                    confirmar = messagebox.askyesno(
                        "Conflito de tecnico",
                        (
                            "Ja existe um tecnico com este TECNICO ENTRADA na base.\n"
                            "Deseja sobrescrever o cadastro existente?"
                        ),
                        parent=modal,
                    )
                    if not confirmar:
                        return

                referencia = {}
                if editando and indice_real is not None and 0 <= indice_real < len(self.tecnicos_base):
                    referencia = dict(self.tecnicos_base[indice_real])
                elif duplicado_idx is not None and 0 <= duplicado_idx < len(self.tecnicos_base):
                    referencia = dict(self.tecnicos_base[duplicado_idx])

                novo = dict(referencia)
                novo.update(
                    {
                        "match": match,
                        "tecnico_saida": tecnico_saida,
                        "estado": estado,
                        "cidade": cidade,
                        "endereco_partida": endereco_partida,
                        "categoria_fixa": categoria_fixa,
                        "horario_inicio_expediente": horario_inicio_expediente,
                        "horario_fim_expediente": horario_fim_expediente,
                        "ativo": True,
                    }
                )
                novo = self._normalizar_regra_base(novo)

                try:
                    if editando and indice_real is not None:
                        if duplicado_idx is not None and duplicado_idx != indice_real:
                            self.tecnicos_base[duplicado_idx] = novo
                            del self.tecnicos_base[indice_real]
                        else:
                            self.tecnicos_base[indice_real] = novo
                    else:
                        if duplicado_idx is not None:
                            self.tecnicos_base[duplicado_idx] = novo
                        else:
                            self.tecnicos_base.append(novo)

                    backup = self._salvar_tecnicos_base()
                    self.atualizar_lista_tecnicos_base()
                    modal.destroy()

                    msg = "Cadastro salvo na base com sucesso."
                    if backup:
                        msg += f"\nBackup: {backup}"
                    messagebox.showinfo("Sucesso", msg)
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao salvar tecnico na base:\n{e}", parent=modal)

            frame_btn = tk.Frame(modal, padx=12, pady=10)
            frame_btn.pack(fill="x")
            ttk.Button(frame_btn, text="Salvar", command=salvar).pack(side="left")
            ttk.Button(frame_btn, text="Cancelar", command=modal.destroy).pack(side="left", padx=8)

        def adicionar_tecnico_base(self):
            self._abrir_modal_tecnico_base(indice_real=None)

        def editar_tecnico_base(self):
            idx_real = self._indice_real_base_selecionado()
            if idx_real is None:
                messagebox.showerror("Erro", "Selecione um tecnico da base para editar.")
                return
            self._abrir_modal_tecnico_base(indice_real=idx_real)

        def ocultar_tecnico_base(self):
            idx_real = self._indice_real_base_selecionado()
            if idx_real is None:
                messagebox.showerror("Erro", "Selecione um tecnico da base para ocultar.")
                return

            tecnico = self.tecnicos_base[idx_real]
            nome = tecnico.get("match", "(SEM MATCH)")
            confirmar = messagebox.askyesno(
                "Confirmacao",
                f"Deseja ocultar o tecnico '{nome}' da base?",
            )
            if not confirmar:
                return

            try:
                self.tecnicos_base[idx_real]["ativo"] = False
                self._salvar_tecnicos_base()
                self.atualizar_lista_tecnicos_base()
                messagebox.showinfo("Sucesso", "Tecnico ocultado com sucesso.")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao ocultar tecnico:\n{e}")

        def listar_tecnicos_ocultos(self):
            ocultos = []
            for idx_real, regra in enumerate(self.tecnicos_base):
                if not self._bool_ativo(regra.get("ativo", True)):
                    ocultos.append((idx_real, regra))

            if not ocultos:
                messagebox.showinfo("Ocultos", "Nao existem tecnicos ocultos na base.")
                return

            modal = tk.Toplevel(self.root)
            modal.title("Tecnicos ocultos")
            modal.geometry("760x360")
            modal.transient(self.root)
            modal.grab_set()

            tk.Label(
                modal,
                text="Tecnicos ocultos na base (selecione um para tornar visivel).",
            ).pack(anchor="w", padx=12, pady=(10, 6))

            frame_filtro_ocultos = tk.Frame(modal)
            frame_filtro_ocultos.pack(fill="x", padx=12, pady=(0, 4))
            tk.Label(frame_filtro_ocultos, text="BUSCAR OCULTO:").pack(side="left")
            filtro_ocultos_var = tk.StringVar()
            ent_filtro_ocultos = ttk.Entry(frame_filtro_ocultos, textvariable=filtro_ocultos_var, width=34)
            ent_filtro_ocultos.pack(side="left", padx=6)

            lista_ocultos = tk.Listbox(modal, height=10, selectmode=tk.SINGLE)
            lista_ocultos.pack(fill="both", expand=True, padx=12, pady=4)

            indices_ocultos = []

            def montar_lista_ocultos():
                lista_ocultos.delete(0, tk.END)
                indices_ocultos.clear()
                filtro = _texto_chave(filtro_ocultos_var.get())
                idx_exibicao = 0
                for idx_real, regra in ocultos:
                    alvo_filtro = _texto_chave(
                        " ".join(
                            [
                                regra.get("match", ""),
                                regra.get("tecnico_saida", ""),
                                regra.get("estado", ""),
                                regra.get("cidade", ""),
                                regra.get("categoria_fixa", ""),
                            ]
                        )
                    )
                    if filtro and filtro not in alvo_filtro:
                        continue
                    idx_exibicao += 1
                    indices_ocultos.append(idx_real)
                    lista_ocultos.insert(tk.END, self._linha_resumo_tecnico(idx_exibicao, regra))

            def limpar_filtro_ocultos():
                filtro_ocultos_var.set("")
                montar_lista_ocultos()

            ent_filtro_ocultos.bind("<KeyRelease>", lambda _e: montar_lista_ocultos())
            ttk.Button(
                frame_filtro_ocultos,
                text="LIMPAR BUSCA",
                command=limpar_filtro_ocultos,
            ).pack(side="left")
            montar_lista_ocultos()

            def tornar_visivel():
                sel = lista_ocultos.curselection()
                if not sel:
                    messagebox.showerror("Erro", "Selecione um tecnico oculto.")
                    return
                idx_real = indices_ocultos[int(sel[0])]
                try:
                    self.tecnicos_base[idx_real]["ativo"] = True
                    self._salvar_tecnicos_base()
                    self.atualizar_lista_tecnicos_base()
                    messagebox.showinfo("Sucesso", "Tecnico reativado com sucesso.")
                    modal.destroy()
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao tornar tecnico visivel:\n{e}")

            frame_btn = tk.Frame(modal, padx=12, pady=10)
            frame_btn.pack(fill="x")
            ttk.Button(frame_btn, text="TORNAR VISIVEL", command=tornar_visivel).pack(side="left")
            ttk.Button(frame_btn, text="Fechar", command=modal.destroy).pack(side="left", padx=8)

        def _chave_bloco_desconhecido(self, item):
            if not isinstance(item, dict):
                return ""
            arquivo = item.get("arquivo_origem", "") or item.get("ARQUIVO ORIGEM", "")
            h = item.get("hash_bloco", "") or item.get("HASH BLOCO", "")
            idx = item.get("indice_bloco", item.get("INDICE BLOCO", ""))
            return f"{arquivo}|{h}|{idx}"

        def limpar_filtro_blocos_desconhecidos(self):
            self.filtro_blocos_desconhecidos_var.set("")
            self.atualizar_lista_blocos_desconhecidos()

        def carregar_blocos_desconhecidos(self):
            eventos = _ler_jsonl(self.arquivo_rats_desconhecidas)
            decisoes = _ler_jsonl(self.arquivo_decisoes_usuario)

            # Mapa de última decisão por bloco.
            decisao_por_chave = {}
            for d in decisoes:
                chave = self._chave_bloco_desconhecido(d)
                if not chave:
                    continue
                atual = decisao_por_chave.get(chave)
                if atual is None or str(d.get("decidido_em", "")) >= str(atual.get("decidido_em", "")):
                    decisao_por_chave[chave] = d

            # Mapa de último evento desconhecido por bloco.
            ultimo_evento = {}
            for ev in eventos:
                chave = self._chave_bloco_desconhecido(ev)
                if not chave:
                    continue
                atual = ultimo_evento.get(chave)
                if atual is None or str(ev.get("processed_at", "")) >= str(atual.get("processed_at", "")):
                    ultimo_evento[chave] = ev

            blocos = []
            for chave, ev in ultimo_evento.items():
                dec = decisao_por_chave.get(chave, {})
                status_decisao = (
                    str(dec.get("decisao_usuario", "")).strip().upper()
                    or str(ev.get("decisao_usuario", "")).strip().upper()
                    or "PENDENTE_CLASSIFICACAO_MANUAL"
                )
                item = dict(ev)
                item["_CHAVE_BLOCO"] = chave
                item["_STATUS_DECISAO"] = status_decisao
                item["_DECIDIDO_POR"] = str(dec.get("decidido_por", "")).strip()
                item["_DECIDIDO_EM"] = str(dec.get("decidido_em", "")).strip()
                blocos.append(item)

            blocos.sort(
                key=lambda x: (
                    str(x.get("processed_at", "")),
                    str(x.get("arquivo_origem", "")),
                    int(x.get("indice_bloco", 0) or 0),
                ),
                reverse=True,
            )
            self.blocos_desconhecidos = blocos
            self.atualizar_lista_blocos_desconhecidos()
            self.registrar_log_interface(
                f"Blocos desconhecidos carregados: {len(self.blocos_desconhecidos)} registro(s)."
            )

        def atualizar_lista_blocos_desconhecidos(self):
            if self.lista_blocos_desconhecidos is None:
                return
            self.lista_blocos_desconhecidos.delete(0, tk.END)
            self.indices_blocos_desconhecidos_visiveis = []
            filtro = _texto_chave(self.filtro_blocos_desconhecidos_var.get())

            pendentes = 0
            for idx_real, item in enumerate(self.blocos_desconhecidos):
                status = str(item.get("_STATUS_DECISAO", "PENDENTE_CLASSIFICACAO_MANUAL")).upper()
                if status == "PENDENTE_CLASSIFICACAO_MANUAL":
                    pendentes += 1
                score = float(item.get("score", 0.0) or 0.0)
                data = str(item.get("data_extraida", "")).strip() or "-"
                chamado = str(item.get("chamado_extraido", "")).strip() or "-"
                tecnico = str(item.get("tecnico_extraido", "")).strip() or "-"
                motivo = str(item.get("motivo", "")).strip() or "-"
                alvo = _texto_chave(f"{status} {data} {chamado} {tecnico} {motivo}")
                if filtro and filtro not in alvo:
                    continue
                self.indices_blocos_desconhecidos_visiveis.append(idx_real)
                linha = (
                    f"[{status}] DATA: {data} | CHAMADO: {chamado} | "
                    f"TÉCNICO: {tecnico} | SCORE: {score:.2f}"
                )
                self.lista_blocos_desconhecidos.insert(tk.END, linha)

            total = len(self.blocos_desconhecidos)
            visiveis = len(self.indices_blocos_desconhecidos_visiveis)
            if filtro:
                self.resumo_blocos_desconhecidos_var.set(
                    (
                        f"BLOCOS DESCONHECIDOS: {visiveis} (filtrado) | "
                        f"TOTAL: {total} | PENDENTES: {pendentes}"
                    )
                )
            else:
                self.resumo_blocos_desconhecidos_var.set(
                    f"BLOCOS DESCONHECIDOS: {total} | PENDENTES: {pendentes}"
                )

        def _indice_bloco_desconhecido_selecionado(self):
            if self.lista_blocos_desconhecidos is None:
                return None
            selecionado = self.lista_blocos_desconhecidos.curselection()
            if not selecionado:
                return None
            idx_visivel = int(selecionado[0])
            if idx_visivel < 0 or idx_visivel >= len(self.indices_blocos_desconhecidos_visiveis):
                return None
            return self.indices_blocos_desconhecidos_visiveis[idx_visivel]

        def _normalizar_campos_ajuste_interface(self, campos):
            if not isinstance(campos, dict):
                return {}
            saida = {}
            for campo, valor in campos.items():
                chave = str(campo or "").strip().upper()
                txt = " ".join(str(valor or "").split()).strip()
                if chave in {"INICIO DA ATIVIDADE", "TÉRMINO DA ATIVIDADE"}:
                    txt = _normalizar_hhmm_interface(txt)
                elif chave in {"KM INICIAL", "KM FINAL"}:
                    txt = re.sub(r"\D", "", txt)
                saida[chave] = txt
            return saida

        def _registrar_decisao_bloco_desconhecido(
            self,
            decisao,
            *,
            campos_ajustados=None,
            observacao="",
        ):
            idx = self._indice_bloco_desconhecido_selecionado()
            if idx is None:
                messagebox.showerror("Erro", "Selecione um bloco desconhecido.")
                return
            item = self.blocos_desconhecidos[idx]
            decisao_txt = str(decisao or "").strip().upper()
            if not decisao_txt:
                return
            campos_norm = self._normalizar_campos_ajuste_interface(campos_ajustados or {})
            evento = {
                "arquivo_origem": item.get("arquivo_origem", ""),
                "hash_bloco": item.get("hash_bloco", ""),
                "indice_bloco": int(item.get("indice_bloco", 0) or 0),
                "decisao_usuario": decisao_txt,
                "decidido_por": _usuario_local(),
                "decidido_em": datetime.now().isoformat(),
                "observacao": str(observacao or "").strip(),
                "campos_ajustados": campos_norm,
            }
            os.makedirs(self.pasta_persistencia, exist_ok=True)
            with open(self.arquivo_decisoes_usuario, "a", encoding="utf-8") as f:
                f.write(json.dumps(evento, ensure_ascii=False) + "\n")
            item["_STATUS_DECISAO"] = decisao_txt
            item["_DECIDIDO_POR"] = evento["decidido_por"]
            item["_DECIDIDO_EM"] = evento["decidido_em"]
            self.atualizar_lista_blocos_desconhecidos()
            self.registrar_log_interface(
                f"Decisão registrada para bloco desconhecido: {decisao_txt}."
            )

        def confirmar_bloco_desconhecido(self):
            self._registrar_decisao_bloco_desconhecido("CONFIRMADO_VALIDO")

        def ignorar_bloco_desconhecido(self):
            self._registrar_decisao_bloco_desconhecido("IGNORAR_BLOCO")

        def reabrir_bloco_desconhecido(self):
            self._registrar_decisao_bloco_desconhecido("PENDENTE_CLASSIFICACAO_MANUAL")

        def ajustar_confirmar_bloco_desconhecido(self):
            idx = self._indice_bloco_desconhecido_selecionado()
            if idx is None:
                messagebox.showerror("Erro", "Selecione um bloco desconhecido.")
                return
            item = self.blocos_desconhecidos[idx]

            modal = tk.Toplevel(self.root)
            modal.title("Ajustar e confirmar bloco desconhecido")
            modal.geometry("860x560")
            modal.transient(self.root)
            modal.grab_set()

            campos = [
                ("DATA", "DATA:"),
                ("CHAMADO", "CHAMADO:"),
                ("TÉCNICO", "TÉCNICO:"),
                ("CLIENTE", "CLIENTE:"),
                ("KM INICIAL", "KM INICIAL:"),
                ("KM FINAL", "KM FINAL:"),
                ("INICIO DA ATIVIDADE", "INICIO DA ATIVIDADE:"),
                ("TÉRMINO DA ATIVIDADE", "TÉRMINO DA ATIVIDADE:"),
                ("ENDEREÇO CLIENTE", "ENDEREÇO CLIENTE:"),
                ("ATIVIDADE REALIZADA", "ATIVIDADE REALIZADA:"),
                ("QUEM ACOMPANHOU", "QUEM ACOMPANHOU:"),
            ]

            valores_iniciais = {
                "DATA": str(item.get("data_extraida", "")).strip(),
                "CHAMADO": str(item.get("chamado_extraido", "")).strip(),
                "TÉCNICO": str(item.get("tecnico_extraido", "")).strip(),
                "CLIENTE": str(item.get("cliente_extraido", "")).strip(),
                "ATIVIDADE REALIZADA": str(item.get("atividade_realizada_extraida", "")).strip(),
            }

            frame_top = tk.Frame(modal, padx=12, pady=10)
            frame_top.pack(fill="x")
            tk.Label(
                frame_top,
                text=(
                    f"BLOCO HASH: {item.get('hash_bloco', '')}\n"
                    f"MOTIVO: {item.get('motivo', '')}\n"
                    f"STATUS ATUAL: {item.get('_STATUS_DECISAO', '')}"
                ),
                justify="left",
                anchor="w",
            ).pack(fill="x")

            frame_form = tk.Frame(modal, padx=12, pady=8)
            frame_form.pack(fill="x")
            vars_form = {}
            for row, (chave, rotulo) in enumerate(campos):
                tk.Label(frame_form, text=rotulo).grid(row=row, column=0, sticky="w", pady=3)
                var = tk.StringVar(value=valores_iniciais.get(chave, ""))
                vars_form[chave] = var
                ttk.Entry(frame_form, textvariable=var, width=84).grid(
                    row=row, column=1, sticky="we", pady=3, padx=(8, 0)
                )
            frame_form.columnconfigure(1, weight=1)

            tk.Label(
                modal,
                text="PREVIEW DO BLOCO:",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=12, pady=(8, 2))
            txt_preview = scrolledtext.ScrolledText(modal, height=9, wrap="word")
            txt_preview.pack(fill="both", expand=True, padx=12, pady=(0, 10))
            txt_preview.insert("1.0", str(item.get("bloco_preview", "")).replace(" | ", "\n"))
            txt_preview.configure(state="disabled")

            def salvar_ajuste():
                campos_ajustados = {}
                for chave, var in vars_form.items():
                    valor = " ".join(str(var.get() or "").split()).strip()
                    if valor:
                        campos_ajustados[chave] = valor
                if not campos_ajustados:
                    messagebox.showerror(
                        "Erro",
                        "Informe ao menos um campo para ajuste antes de confirmar.",
                        parent=modal,
                    )
                    return
                self._registrar_decisao_bloco_desconhecido(
                    "AJUSTADO_CONFIRMADO",
                    campos_ajustados=campos_ajustados,
                    observacao="AJUSTE MANUAL VIA CAMPO AVANCADO",
                )
                modal.destroy()

            frame_btn = tk.Frame(modal, padx=12, pady=10)
            frame_btn.pack(fill="x")
            ttk.Button(frame_btn, text="SALVAR AJUSTE E CONFIRMAR", command=salvar_ajuste).pack(side="left")
            ttk.Button(frame_btn, text="CANCELAR", command=modal.destroy).pack(side="left", padx=8)

        def ver_bloco_desconhecido(self):
            idx = self._indice_bloco_desconhecido_selecionado()
            if idx is None:
                messagebox.showerror("Erro", "Selecione um bloco desconhecido.")
                return
            item = self.blocos_desconhecidos[idx]

            modal = tk.Toplevel(self.root)
            modal.title("Detalhes do bloco desconhecido")
            modal.geometry("920x520")
            modal.transient(self.root)
            modal.grab_set()

            frame_info = tk.Frame(modal, padx=12, pady=10)
            frame_info.pack(fill="x")

            info_txt = (
                f"STATUS: {item.get('_STATUS_DECISAO', '')}\n"
                f"DATA: {item.get('data_extraida', '')}\n"
                f"CHAMADO: {item.get('chamado_extraido', '')}\n"
                f"TÉCNICO: {item.get('tecnico_extraido', '')}\n"
                f"CLIENTE: {item.get('cliente_extraido', '')}\n"
                f"SCORE: {item.get('score', '')} | LIMIAR: {item.get('limiar', '')}\n"
                f"MOTIVO: {item.get('motivo', '')}\n"
                f"PADRÃO SUGERIDO: {item.get('padrao_sugerido_nome', '')} ({item.get('padrao_sugerido_id', '')})\n"
                f"HASH BLOCO: {item.get('hash_bloco', '')}\n"
                f"ARQUIVO ORIGEM: {item.get('arquivo_origem', '')}\n"
                f"ÍNDICE BLOCO: {item.get('indice_bloco', '')}\n"
                f"DECIDIDO POR: {item.get('_DECIDIDO_POR', '')} | EM: {item.get('_DECIDIDO_EM', '')}"
            )
            tk.Label(frame_info, text=info_txt, justify="left", anchor="w").pack(fill="x")

            tk.Label(
                modal,
                text="PREVIEW DO BLOCO:",
                font=("Segoe UI", 9, "bold"),
            ).pack(anchor="w", padx=12, pady=(8, 2))

            txt_preview = scrolledtext.ScrolledText(modal, height=14, wrap="word")
            txt_preview.pack(fill="both", expand=True, padx=12, pady=(0, 10))
            txt_preview.insert(
                "1.0",
                str(item.get("bloco_preview", "")).replace(" | ", "\n"),
            )
            txt_preview.configure(state="disabled")

            frame_btn = tk.Frame(modal, padx=12, pady=10)
            frame_btn.pack(fill="x")
            ttk.Button(frame_btn, text="FECHAR", command=modal.destroy).pack(side="left")

        def selecionar_pasta(self):
            # Adiciona todos os .txt da pasta selecionada (incluindo subpastas).
            pasta = filedialog.askdirectory()
            if not pasta:
                return

            encontrados = []
            for raiz, _, arquivos in os.walk(pasta):
                for nome in arquivos:
                    if nome.lower().endswith(".txt"):
                        encontrados.append(os.path.join(raiz, nome))

            if not encontrados:
                messagebox.showinfo("Aviso", "Nenhum arquivo TXT encontrado nesta pasta.")
                return

            self.adicionar_arquivos(encontrados)

        def adicionar_arquivos(self, arquivos):
            # Centraliza a insercao para evitar duplicados na lista.
            qtd_novos = 0
            for arq in arquivos:
                if arq not in self.arquivos:
                    self.arquivos.append(arq)
                    self.lista.insert(tk.END, arq)
                    qtd_novos += 1
            if qtd_novos:
                self.registrar_log_interface(f"{qtd_novos} arquivo(s) adicionados à lista.")
            self.atualizar_resumo_execucao()

        def remover_arquivo_selecionado(self):
            # Remove da lista visual e da lista interna os itens selecionados.
            selecionados = list(self.lista.curselection())
            if not selecionados:
                return
            qtd_removidos = 0
            for idx in reversed(selecionados):
                caminho = self.lista.get(idx)
                self.lista.delete(idx)
                if caminho in self.arquivos:
                    self.arquivos.remove(caminho)
                    qtd_removidos += 1
            if qtd_removidos:
                self.registrar_log_interface(f"{qtd_removidos} arquivo(s) removidos da lista.")
            self.atualizar_resumo_execucao()

        def limpar_lista_arquivos(self):
            # Limpa toda a selecao de arquivos.
            self.arquivos = []
            self.lista.delete(0, tk.END)
            self.registrar_log_interface("Lista de arquivos limpa.")
            self.atualizar_resumo_execucao()

        def selecionar_saida(self):
            # Define o caminho do excel final e atualiza estado do botao de pasta.
            caminho = filedialog.asksaveasfilename(defaultextension=".xlsx")
            if caminho:
                self.saida_var.set(caminho)
                self.atualizar_botao_pasta()
                self.registrar_log_interface(f"Arquivo de saída definido: {caminho}")
                self.atualizar_resumo_execucao()

        def atualizar_botao_pasta(self):
            # Habilita o botao somente quando existe caminho valido informado.
            caminho_saida = self.saida_var.get().strip()
            if caminho_saida:
                self.btn_pasta.configure(state="normal")
            else:
                self.btn_pasta.configure(state="disabled")
            if self.btn_abrir_excel is not None:
                if caminho_saida and os.path.isfile(caminho_saida):
                    self.btn_abrir_excel.configure(state="normal")
                else:
                    self.btn_abrir_excel.configure(state="disabled")

        def abrir_pasta_saida(self):
            # Abre no Explorer a pasta do arquivo de saida.
            caminho_saida = self.saida_var.get().strip()
            if not caminho_saida:
                messagebox.showerror("Erro", "Defina o arquivo de saida primeiro.")
                return

            pasta = os.path.dirname(caminho_saida) or os.getcwd()
            if not os.path.isdir(pasta):
                messagebox.showerror("Erro", "A pasta de saida nao existe.")
                return

            os.startfile(pasta)

        def executar(self):
            # Aciona processamento, mede tempo total e informa resultado ao usuario.
            if self.execucao_em_andamento:
                messagebox.showwarning("Aguarde", "Já existe um processamento em andamento.")
                return

            if not self.arquivos:
                messagebox.showerror("Erro", "Selecione os arquivos TXT")
                return

            if not self.saida_var.get().strip():
                messagebox.showerror("Erro", "Defina o arquivo de saida")
                return

            try:
                self.execucao_em_andamento = True
                self.inicio_execucao = time.perf_counter()
                self.progress.start()
                if self.btn_gerar is not None:
                    self.btn_gerar.configure(state="disabled")
                self.root.update_idletasks()
                self.registrar_log_interface("Processamento iniciado.")
                self.tempo_processamento_var.set("Tempo de processamento: processando...")

                params = {
                    "arquivos": list(self.arquivos),
                    "saida": self.saida_var.get().strip(),
                    "data_inicio": self.data_inicio_var.get().strip(),
                    "data_fim": self.data_fim_var.get().strip(),
                    "filtro_tecnico": self.filtro_tecnico_var.get().strip(),
                    "filtro_status": self.filtro_status_var.get().strip(),
                    "filtro_cidade": self.filtro_cidade_var.get().strip(),
                    "somente_inconsistencias": self.somente_incons_var.get(),
                    "regras_tecnicos_extra": list(self.regras_tecnicos_sessao),
                }

                worker = threading.Thread(
                    target=self._executar_worker_excel,
                    args=(params,),
                    daemon=True,
                )
                worker.start()
                self.root.after(150, self._monitorar_execucao_worker)

            except Exception as e:
                self.execucao_em_andamento = False
                if self.btn_gerar is not None:
                    self.btn_gerar.configure(state="normal")
                self.progress.stop()
                self.registrar_log_interface(f"Erro no processamento: {e}")
                messagebox.showerror("Erro", str(e))

        def _executar_worker_excel(self, params):
            # Roda extrator em thread separada para nao travar a GUI.
            try:
                resumo = gerar_excel(
                    params["arquivos"],
                    params["saida"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                    filtro_tecnico=params["filtro_tecnico"],
                    filtro_status=params["filtro_status"],
                    filtro_cidade=params["filtro_cidade"],
                    somente_inconsistencias=params["somente_inconsistencias"],
                    regras_tecnicos_extra=params["regras_tecnicos_extra"],
                )
                self.fila_execucao.put(("SUCESSO", resumo, ""))
            except Exception as exc:
                detalhe = "".join(traceback.format_exception_only(type(exc), exc)).strip()
                self.fila_execucao.put(("ERRO", None, detalhe))

        def _monitorar_execucao_worker(self):
            # Consulta fila do worker e finaliza a execucao no thread da GUI.
            try:
                status, resumo, erro = self.fila_execucao.get_nowait()
            except queue.Empty:
                if self.execucao_em_andamento:
                    self.root.after(200, self._monitorar_execucao_worker)
                return

            self.execucao_em_andamento = False
            self.progress.stop()
            if self.btn_gerar is not None:
                self.btn_gerar.configure(state="normal")

            duracao = max(time.perf_counter() - self.inicio_execucao, 0.0)
            duracao_txt = _formatar_duracao_hms(duracao)
            self.tempo_processamento_var.set(f"Tempo de processamento: {duracao_txt}")
            self.atualizar_botao_pasta()

            if status == "SUCESSO":
                self.atualizar_painel_resultado(resumo, duracao)
                self.carregar_blocos_desconhecidos()
                self.registrar_log_interface(
                    (
                        f"Processamento concluído em {duracao_txt}. "
                        f"Registros gerados: {resumo.get('registros_gerados', '-') if isinstance(resumo, dict) else '-'}."
                    )
                )
                messagebox.showinfo("Sucesso", f"Arquivo gerado com sucesso!\nTempo: {duracao_txt}")
                return

            self.registrar_log_interface(f"Erro no processamento: {erro}")
            messagebox.showerror("Erro", erro or "Falha desconhecida no processamento.")

# ==========================
# BLOCO 4: MODO CLI (FALLBACK)
# ==========================
# Fluxo usado automaticamente quando nao ha suporte grafico.
else:

    def executar_cli():
        print("Tkinter nao disponivel. Rodando em modo terminal.\n")

        arquivos = input("Digite os caminhos dos arquivos TXT (separados por virgula):\n> ").split(",")
        arquivos = [a.strip() for a in arquivos if a.strip()]

        if not arquivos:
            print("Erro: nenhum arquivo informado")
            return

        saida = input("Digite o caminho do arquivo Excel de saida:\n> ").strip()
        if not saida:
            print("Erro: saida nao informada")
            return

        data_inicio = input("Data inicial (dd/mm/aaaa) [opcional]:\n> ").strip()
        data_fim = input("Data final (dd/mm/aaaa) [opcional]:\n> ").strip()
        filtro_tecnico = input("Filtro técnico [opcional]:\n> ").strip()
        filtro_status = input("Filtro status [opcional]:\n> ").strip()
        filtro_cidade = input("Filtro cidade [opcional]:\n> ").strip()
        somente_incons = input("Somente inconsistências? (s/n) [opcional]:\n> ").strip()

        try:
            inicio = time.perf_counter()
            gerar_excel(
                arquivos,
                saida,
                data_inicio=data_inicio,
                data_fim=data_fim,
                filtro_tecnico=filtro_tecnico,
                filtro_status=filtro_status,
                filtro_cidade=filtro_cidade,
                somente_inconsistencias=somente_incons,
            )
            duracao = time.perf_counter() - inicio
            print(f"Excel gerado com sucesso em {_formatar_duracao_hms(duracao)}!")
        except Exception as e:
            print(f"Erro: {e}")

# ==========================
# BLOCO 5: TESTE RAPIDO LOCAL
# ==========================
# Valida rapidamente a chamada principal de exportacao.
def _teste_basico():
    try:
        gerar_excel(["teste1.txt"], "teste_saida.xlsx")
        print("Teste basico OK")
    except Exception as e:
        print("Teste falhou:", e)

# ==========================
# BLOCO 6: PONTO DE ENTRADA
# ==========================
# Decide entre GUI e CLI com base na disponibilidade do Tkinter.
if __name__ == "__main__":
    if GUI_DISPONIVEL:
        root = tk.Tk()
        style = ttk.Style()
        style.theme_use("clam")
        app = App(root)
        root.mainloop()
    else:
        executar_cli()
