"""
Extrator de RATs a partir de arquivos TXT.

Resumo do fluxo:
1) carrega regras externas (tecnicos, categorias e enderecos);
2) encontra blocos de RAT e extrai os campos em diferentes formatos;
3) aplica regras de negocio e normalizacao;
4) gera Excel com abas DADOS e LOG.
"""

import copy
import json
import math
import os
import re
import unicodedata
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

# -------------------------
# BLOCO A: CONSTANTES E REGRAS BASE
# -------------------------
# CAMPOS define a ordem final de colunas da planilha.
CAMPOS = [
    "DATA",
    "CHAMADO",
    "CLIENTE",
    "ESTADO",
    "CIDADE",
    "TÉCNICO",
    "DESCRIÇÃO DO CHAMADO",
    "KM INICIAL",
    "KM FINAL",
    "INICIO DA ATIVIDADE",
    "TÉRMINO DA ATIVIDADE",
    "ENDEREÇO DE PARTIDA",
    "ENDEREÇO CLIENTE",
    "ATIVIDADE REALIZADA",
    "PATRIMÔNIO",
    "CATEGORIA",
    "STATUS",
    "QUEM ACOMPANHOU",
]

RETORNO_CLIENTE_FIXO = "BASE AVANÇADA"
RETORNO_DESCRICAO_FIXA = "BASE AVANÇADA"
RETORNO_ATIVIDADE_FIXA = "RETORNO"
RETORNO_STATUS_FIXO = "RESOLVIDO"
RETORNO_QUEM_ACOMPANHOU_FIXO = "FERNADO"
EXPEDIENTE_INICIO_PADRAO = "08:00"
EXPEDIENTE_FIM_PADRAO = "18:00"
ROTA_TIMEOUT_SEGUNDOS = 2
ROTA_MAX_TENTATIVAS = 2000

_CACHE_GEO = {}
_CACHE_ROTA = {}
_ROTA_TENTATIVAS = 0
_SERVICO_ROTA_INDISPONIVEL = False

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "regras_config.json")
CONFIG_DIR_PATH = os.path.join(os.path.dirname(__file__), "regras")
CONFIG_TECNICOS_PATH = os.path.join(CONFIG_DIR_PATH, "tecnicos_regras.json")
CONFIG_CATEGORIAS_PATH = os.path.join(CONFIG_DIR_PATH, "categorias.json")
CONFIG_STATUS_PATH = os.path.join(CONFIG_DIR_PATH, "status.json")
CONFIG_KM_PATH = os.path.join(CONFIG_DIR_PATH, "km.json")
CONFIG_ENDERECO_PATH = os.path.join(CONFIG_DIR_PATH, "endereco.json")
CONFIG_QUALIDADE_PATH = os.path.join(CONFIG_DIR_PATH, "qualidade.json")
CONFIG_FILTROS_PATH = os.path.join(CONFIG_DIR_PATH, "filtros.json")
DEFAULT_CONFIG = {
    "tecnicos_regras": [
        {
            "match": "glaydson",
            "estado": "CE",
            "cidade": "Fortaleza",
            "endereco_partida": "Av. Des. Moreira, 1300 - Aldeota, Fortaleza - CE",
            "tecnico_saida": "CE_GLAYDSON_930.024.097-80",
        },
        {
            "match": "edilberto",
            "estado": "PA",
            "cidade": "Belém",
            "endereco_partida": "Av. Gov Magalhães Barata, 651 - São Brás, Belém - PA",
            "tecnico_saida": "PA_EDILBERTO_6067225",
        },
        {
            "match": "acacio",
            "estado": "RJ",
            "cidade": "Rio de Janeiro",
            "endereco_partida": "CEO Corporate Executive Office - Barra da Tijuca, Rio de Janeiro - RJ",
            "tecnico_saida": "RJ_ACACIO_27.962.560-2",
        },
        {
            "match": "robson santos",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_CR_ROBSON SANTOS_22.194.425",
        },
        {
            "match": "robson paulo",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_CR_ROBSON SANTOS_22.194.425",
        },
        {
            "match": "robson marques",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "IMP_ROBSON_40.266.824-8",
            "categoria_fixa": "IMPRESSORA LASER",
        },
        {
            "match": "izak",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "IMP_IZAK DANTAS_50.454.079-8",
            "categoria_fixa": "IMPRESSORA LASER",
        },
        {
            "match": "joao augusto",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "IMP_JOAO AUGUSTO_406.370.468-89",
            "categoria_fixa": "IMPRESSORA LASER",
        },
        {
            "match": "gustavo",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "IMP_GUSTAVO_37.086.622-8",
            "categoria_fixa": "IMPRESSORA LASER",
        },
        {
            "match": "breno",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_CR_BRENO LUCINDO_58.637.346-9",
        },
        {
            "match": "joao vitor",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_MT_JOAO VITOR_37241087-X",
        },
        {
            "match": "alan",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_MT_ALAN GOMES_391643003",
        },
        {
            "match": "gabriel",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_MT_GABRIEL DE SOUZA GUAROS_589409840",
        },
        {
            "match": "cristian",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_MT_CRISTIAN ALEXANDRE_590916269",
        },
        {
            "match": "marcelo",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "SP_CR_MARCELO HIDEO ISERI_32.398.982-2",
        },
        {
            "match": "mario",
            "estado": "SP",
            "cidade": "São Paulo",
            "endereco_partida": "Av. Marquês de S. Vicente, 576 - Várzea da Barra Funda",
            "tecnico_saida": "",
        },
    ],
    "categoria_ordem": [
        "IMPRESSORA",
        "NOBREAK",
        "NOTEBOOK",
        "DESKTOP",
        "PERIFERICO",
    ],
    "categoria_palavras_chave": {
        "IMPRESSORA": [
            "impressora",
            "bobina",
            "elgin i9",
            "elgin",
            "termica",
            "impressao",
            "toner",
            "cartucho",
            "etiqueta",
            "zebra",
            "argox",
            "bematech",
            "nao imprime",
        ],
        "NOBREAK": ["nobreak", "no-break", "no break", "ups"],
        "NOTEBOOK": ["notebook", "laptop", "tablet", "ultrabook", "macbook"],
        "DESKTOP": [
            "pc",
            "desktop",
            "computador",
            "cpu",
            "pdv",
            "terminal",
            "all in one",
            "workstation",
            "totem",
            "gabinete",
            "mini pc",
        ],
        "PERIFERICO": [
            "headset",
            "fonte",
            "teclado",
            "mouse",
            "periferico",
            "monitor",
            "carregador",
            "webcam",
            "dock",
            "cabo",
            "adaptador",
            "scanner",
            "leitor",
            "mousepad",
            "bateria",
            "microfone",
            "caixa de som",
            "caixa som",
            "hdmi",
        ],
    },
    "categoria_padrao": "PERIFERICO",
    "status_regras": {
        "palavras_improdutivo": ["improdutivo"],
        "palavras_avaliacao": ["pendente"],
        "status_improdutivo": "IMPRODUTIVO",
        "status_padrao": "RESOLVIDO",
        "descricao_improdutivo": "IMPRODUTIVO",
        "descricao_avaliacao": "AVALIAÇÃO",
        "descricao_padrao": "MANUTENÇÃO",
    },
    "km_regras": {
        "limpar_tokens_exatos": ["*", "o"],
        "limpar_regex_norm": ["^x{1,10}$", "^a\\s*pe$"],
        "palavra_uber": "uber",
        "remover_asterisco": True,
        "manter_apenas_digitos": True,
        "incremento_km_final_ausente": 10,
        "limpar_quando_uber": True,
        "mascarar_repetido_mesmo_dia": True,
    },
    "endereco_regras": {
        "mascara_valor": "-",
        "mascarar_repetido_mesmo_dia": True,
        "mascarar_iguais_no_registro": True,
    },
    "qualidade_regras": {
        "nome_regra_log": "VALIDACAO QUALIDADE",
        "prefixo_inconsistente": "INCONSISTENTE:",
        "validar_data": True,
        "validar_chamado": True,
        "regex_chamado": r"^\d{1,20}$",
        "validar_tecnico_vazio": True,
        "validar_tecnico_mapeado": True,
        "motivo_data_invalida": "DATA INVALIDA",
        "motivo_chamado_invalido": "CHAMADO VAZIO OU INVALIDO",
        "motivo_tecnico_vazio": "TECNICO VAZIO",
        "motivo_tecnico_nao_mapeado": "TECNICO NAO MAPEADO",
    },
    "filtros_regras": {
        "somente_inconsistencias_true_values": ["1", "true", "sim", "s", "yes", "y"],
        "usar_contains_tecnico": True,
        "usar_contains_status": True,
        "usar_contains_cidade": True,
        "ignorar_logs_dedup_com_filtro_status_cidade": True,
    },
}

WHATSAPP_PREFIXO_RE = re.compile(
    r"^\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}\s*-\s*[^:]+:\s*"
)

CAMPOS_MULTILINHA = {"ENDEREÇO CLIENTE", "ATIVIDADE REALIZADA"}
CAMPOS_VALOR_LINHA_SEGUINTE = {
    "TÉCNICO",
    "DATA",
    "CLIENTE",
    "CHAMADO",
    "KM INICIAL",
    "KM FINAL",
    "PREVISAO CHEGADA",
    "INICIO ATIVIDADE",
    "TÉRMINO DA ATIVIDADE",
    "STATUS ORIGINAL",
    "QUEM ACOMPANHOU",
}
CAMPOS_NAO_INFORMADO = {
    "TÉCNICO",
    "ESTADO",
    "CIDADE",
    "INICIO DA ATIVIDADE",
    "ATIVIDADE REALIZADA",
    "QUEM ACOMPANHOU",
}
COLUNAS_LOG = [
    "ARQUIVO ORIGEM",
    "DATA",
    "CHAMADO",
    "TÉCNICO",
    "REGRA",
    "CAMPO",
    "VALOR ANTERIOR",
    "VALOR FINAL",
]

# Nomes finais (corrigidos) para exportacao em Excel.
CAMPOS_EXPORTACAO = [
    "DATA",
    "CHAMADO",
    "CLIENTE",
    "ESTADO",
    "CIDADE",
    "TÉCNICO",
    "DESCRIÇÃO DO CHAMADO",
    "KM INICIAL",
    "KM FINAL",
    "KM PERCORRIDO",
    "INICIO DA ATIVIDADE",
    "TÉRMINO DA ATIVIDADE",
    "ENDEREÇO DE PARTIDA",
    "ENDEREÇO CLIENTE",
    "ATIVIDADE REALIZADA",
    "PATRIMÔNIO",
    "CATEGORIA",
    "STATUS",
    "QUEM ACOMPANHOU",
]

COLUNAS_LOG_EXPORTACAO = [
    "ARQUIVO ORIGEM",
    "DATA",
    "CHAMADO",
    "TÉCNICO",
    "REGRA",
    "CAMPO",
    "VALOR ANTERIOR",
    "VALOR FINAL",
]

# Mapeamento explicito entre chaves internas e colunas de saida.
# Evita dependencia de alinhamento por indice entre listas.
MAPA_CAMPOS_EXPORTACAO = {
    "DATA": "DATA",
    "CHAMADO": "CHAMADO",
    "CLIENTE": "CLIENTE",
    "ESTADO": "ESTADO",
    "CIDADE": "CIDADE",
    "TÉCNICO": "TÉCNICO",
    "DESCRIÇÃO DO CHAMADO": "DESCRIÇÃO DO CHAMADO",
    "KM INICIAL": "KM INICIAL",
    "KM FINAL": "KM FINAL",
    "KM PERCORRIDO": "KM PERCORRIDO",
    "INICIO DA ATIVIDADE": "INICIO DA ATIVIDADE",
    "TÉRMINO DA ATIVIDADE": "TÉRMINO DA ATIVIDADE",
    "ENDEREÇO DE PARTIDA": "ENDEREÇO DE PARTIDA",
    "ENDEREÇO CLIENTE": "ENDEREÇO CLIENTE",
    "ATIVIDADE REALIZADA": "ATIVIDADE REALIZADA",
    "PATRIMÔNIO": "PATRIMÔNIO",
    "CATEGORIA": "CATEGORIA",
    "STATUS": "STATUS",
    "QUEM ACOMPANHOU": "QUEM ACOMPANHOU",
}

MAPA_COLUNAS_LOG_EXPORTACAO = {
    "ARQUIVO ORIGEM": "ARQUIVO ORIGEM",
    "DATA": "DATA",
    "CHAMADO": "CHAMADO",
    "TÉCNICO": "TÉCNICO",
    "REGRA": "REGRA",
    "CAMPO": "CAMPO",
    "VALOR ANTERIOR": "VALOR ANTERIOR",
    "VALOR FINAL": "VALOR FINAL",
}

# Camada de compatibilidade: aceita chaves antigas (mojibake) sem quebrar o fluxo.
CHAVES_LEGADAS = {
    "TÃ‰CNICO": "TÉCNICO",
    "DESCRIÃ‡ÃƒO DO CHAMADO": "DESCRIÇÃO DO CHAMADO",
    "TÃ‰RMINO DA ATIVIDADE": "TÉRMINO DA ATIVIDADE",
    "ENDEREÃ‡O DE PARTIDA": "ENDEREÇO DE PARTIDA",
    "ENDEREÃ‡O CLIENTE": "ENDEREÇO CLIENTE",
    "PATRIMÃ”NIO": "PATRIMÔNIO",
    "CHEGADA": "INICIO DA ATIVIDADE",
}
MAPA_CAMPOS_EXPORTACAO.update(CHAVES_LEGADAS)
MAPA_COLUNAS_LOG_EXPORTACAO["TÃ‰CNICO"] = "TÉCNICO"


def aplicar_compatibilidade_chaves(registro):
    if not isinstance(registro, dict):
        return registro
    for chave_legada, chave_nova in CHAVES_LEGADAS.items():
        if chave_legada in registro and chave_nova not in registro:
            registro[chave_nova] = registro.get(chave_legada, "")
    return registro


# -------------------------
# BLOCO B1: VALIDACAO DE QUALIDADE E FILTROS AVANCADOS
# -------------------------
# Sinaliza campos criticos invalidos e aplica filtros opcionais de exportacao.
def registrar_inconsistencia_qualidade(linha, campo, valor_atual, motivo):
    regra_nome = QUALIDADE_REGRAS.get("nome_regra_log", "VALIDACAO QUALIDADE")
    prefixo = QUALIDADE_REGRAS.get("prefixo_inconsistente", "INCONSISTENTE:")
    descricao = limpar(f"{prefixo} {motivo}")
    registrar_alteracao_linha(
        linha,
        regra_nome,
        campo,
        valor_atual,
        descricao,
    )
    linha["_TEM_INCONSISTENCIA"] = True


def validar_qualidade_registro(linha, tecnicos_regras=None):
    data_txt = limpar(linha.get("DATA", ""))
    if QUALIDADE_REGRAS.get("validar_data", True):
        if not data_txt or data_para_date(data_txt) is None:
            registrar_inconsistencia_qualidade(
                linha,
                "DATA",
                data_txt,
                QUALIDADE_REGRAS.get("motivo_data_invalida", "DATA INVALIDA"),
            )

    chamado_txt = limpar(linha.get("CHAMADO", ""))
    tipo_registro = norm(linha.get("_TIPO_REGISTRO", ""))
    validar_chamado = QUALIDADE_REGRAS.get("validar_chamado", True) and tipo_registro != "retorno_base"
    if validar_chamado:
        regex_chamado = QUALIDADE_REGRAS.get("regex_chamado", r"^\d{1,20}$")
        # CHAMADO em branco pode ser válido conforme padrão de RAT.
        # Quando preenchido, valida o formato para evitar ruído.
        if chamado_txt and not re.fullmatch(regex_chamado, chamado_txt):
            registrar_inconsistencia_qualidade(
                linha,
                "CHAMADO",
                chamado_txt,
                QUALIDADE_REGRAS.get("motivo_chamado_invalido", "CHAMADO VAZIO OU INVALIDO"),
            )

    tecnico_txt = limpar(linha.get("TÉCNICO", ""))
    if QUALIDADE_REGRAS.get("validar_tecnico_vazio", True) and not tecnico_txt:
        registrar_inconsistencia_qualidade(
            linha,
            "TÉCNICO",
            tecnico_txt,
            QUALIDADE_REGRAS.get("motivo_tecnico_vazio", "TECNICO VAZIO"),
        )
    elif (
        tecnico_txt
        and QUALIDADE_REGRAS.get("validar_tecnico_mapeado", True)
        and regra_tecnico(tecnico_txt, tecnicos_regras=tecnicos_regras) is None
    ):
        registrar_inconsistencia_qualidade(
            linha,
            "TÉCNICO",
            tecnico_txt,
            QUALIDADE_REGRAS.get("motivo_tecnico_nao_mapeado", "TECNICO NAO MAPEADO"),
        )


def registro_passa_filtros(
    registro,
    filtro_tecnico="",
    filtro_status="",
    filtro_cidade="",
    somente_inconsistencias=False,
):
    tecnico_filtro = norm(filtro_tecnico)
    status_filtro = norm(filtro_status)
    cidade_filtro = norm(filtro_cidade)

    tecnico_reg = norm(registro.get("TÉCNICO", ""))
    status_reg = norm(registro.get("STATUS", ""))
    cidade_reg = norm(registro.get("CIDADE", ""))

    if tecnico_filtro and (
        tecnico_filtro != tecnico_reg
        if not FILTROS_REGRAS.get("usar_contains_tecnico", True)
        else tecnico_filtro not in tecnico_reg
    ):
        return False
    if status_filtro and (
        status_filtro != status_reg
        if not FILTROS_REGRAS.get("usar_contains_status", True)
        else status_filtro not in status_reg
    ):
        return False
    if cidade_filtro and (
        cidade_filtro != cidade_reg
        if not FILTROS_REGRAS.get("usar_contains_cidade", True)
        else cidade_filtro not in cidade_reg
    ):
        return False
    if somente_inconsistencias and not registro.get("_TEM_INCONSISTENCIA", False):
        return False
    return True


# -------------------------
# BLOCO B: UTILITARIOS DE TEXTO
# -------------------------
# Base para comparar textos com robustez (acento, caixa, espacos).
def corrigir_mojibake(txt):
    if txt is None:
        return ""

    s = str(txt).replace("\u00a0", " ")
    marcadores = ("Ã", "Â", "�")
    if not any(m in s for m in marcadores):
        return s

    original = s
    for enc in ("latin-1", "cp1252"):
        try:
            candidato = s.encode(enc).decode("utf-8")
            antes = sum(original.count(m) for m in marcadores)
            depois = sum(candidato.count(m) for m in marcadores)
            if depois < antes:
                s = candidato
                break
        except Exception:
            continue
    return s


def norm(txt):
    if txt is None:
        return ""
    txt = corrigir_mojibake(txt)
    txt = unicodedata.normalize("NFD", txt)
    txt = txt.encode("ascii", "ignore").decode()
    return txt.lower().strip()


def limpar(txt):
    if txt is None:
        return ""
    txt = corrigir_mojibake(txt)
    txt = re.sub(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]", "", txt)
    return re.sub(r"\s+", " ", str(txt).replace("\u00a0", " ")).strip()


def normalizar_hhmm_basico(valor, padrao=""):
    txt = limpar(valor)
    if not txt:
        return limpar(padrao)

    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", txt.replace(";", ":").replace(".", ":"))
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"

    return limpar(padrao)


# -------------------------
# BLOCO C: NORMALIZACAO DA CONFIG EXTERNA
# -------------------------
# Prepara estrutura de tecnicos e palavras-chave para lookup rapido.
def _normalizar_tecnicos_regras(regras, origem_padrao="CONFIG"):
    def _bool_ativo(valor):
        if isinstance(valor, bool):
            return valor
        if valor is None:
            return True
        txt = norm(valor)
        if txt in {"0", "false", "falso", "nao", "não", "n", "off"}:
            return False
        return True

    saida = []
    if not isinstance(regras, list):
        return saida
    for item in regras:
        if not isinstance(item, dict):
            continue
        if not _bool_ativo(item.get("ativo", True)):
            continue
        match = limpar(item.get("match", ""))
        if not match:
            continue
        origem = limpar(item.get("origem", origem_padrao)).upper() or "CONFIG"
        prioridade = 1 if origem == "UI_AVANCADO" else 0
        saida.append(
            {
                "match": match,
                "match_norm": norm(match),
                "estado": limpar(item.get("estado", "")),
                "cidade": limpar(item.get("cidade", "")),
                "endereco_partida": limpar(item.get("endereco_partida", "")),
                "tecnico_saida": limpar(item.get("tecnico_saida", "")),
                "categoria_fixa": limpar(item.get("categoria_fixa", "")),
                "horario_inicio_expediente": normalizar_hhmm_basico(
                    item.get("horario_inicio_expediente", ""),
                    EXPEDIENTE_INICIO_PADRAO,
                ),
                "horario_fim_expediente": normalizar_hhmm_basico(
                    item.get("horario_fim_expediente", ""),
                    EXPEDIENTE_FIM_PADRAO,
                ),
                "origem": origem,
                "prioridade": prioridade,
            }
        )
    # Prioridade:
    # 1) match mais específico (maior texto normalizado)
    # 2) origem (UI_AVANCADO vence CONFIG em empate de especificidade)
    saida.sort(key=lambda x: (len(x["match_norm"]), x["prioridade"]), reverse=True)
    return saida


def montar_tecnicos_regras_ativas(regras_tecnicos_extra=None):
    regras_base = copy.deepcopy(CONFIG_REGRAS.get("tecnicos_regras", []))
    regras_extra = []
    if isinstance(regras_tecnicos_extra, list):
        for item in regras_tecnicos_extra:
            if not isinstance(item, dict):
                continue
            regras_extra.append(
                {
                    "match": item.get("match", ""),
                    "estado": item.get("estado", ""),
                    "cidade": item.get("cidade", ""),
                    "endereco_partida": item.get("endereco_partida", ""),
                    "tecnico_saida": item.get("tecnico_saida", ""),
                    "categoria_fixa": item.get("categoria_fixa", ""),
                    "horario_inicio_expediente": item.get("horario_inicio_expediente", ""),
                    "horario_fim_expediente": item.get("horario_fim_expediente", ""),
                    "origem": "UI_AVANCADO",
                }
            )
    return _normalizar_tecnicos_regras(regras_base + regras_extra)


def _normalizar_categoria_palavras(cfg):
    origem = cfg.get("categoria_palavras_chave", {})
    if not isinstance(origem, dict):
        origem = {}

    palavras = {}
    for categoria, lista in origem.items():
        if not isinstance(lista, list):
            continue
        itens = [norm(x) for x in lista if limpar(x)]
        if itens:
            palavras[categoria.upper()] = itens
    return palavras


def _normalizar_lista_textos_norm(valor):
    itens = []
    if isinstance(valor, str):
        itens = [valor]
    elif isinstance(valor, list):
        itens = valor

    saida = []
    for item in itens:
        n = norm(item)
        if n:
            saida.append(n)
    return saida


def _normalizar_status_regras(cfg):
    base = copy.deepcopy(DEFAULT_CONFIG.get("status_regras", {}))
    if isinstance(cfg, dict):
        for chave in list(base.keys()):
            if chave in cfg:
                base[chave] = cfg.get(chave)

    base["palavras_improdutivo"] = _normalizar_lista_textos_norm(
        base.get("palavras_improdutivo", [])
    )
    base["palavras_avaliacao"] = _normalizar_lista_textos_norm(base.get("palavras_avaliacao", []))
    base["status_improdutivo"] = limpar(base.get("status_improdutivo", "IMPRODUTIVO")).upper()
    base["status_padrao"] = limpar(base.get("status_padrao", "RESOLVIDO")).upper()
    base["descricao_improdutivo"] = limpar(base.get("descricao_improdutivo", "IMPRODUTIVO")).upper()
    base["descricao_avaliacao"] = limpar(base.get("descricao_avaliacao", "AVALIAÇÃO")).upper()
    base["descricao_padrao"] = limpar(base.get("descricao_padrao", "MANUTENÇÃO")).upper()
    return base


def _normalizar_km_regras(cfg):
    base = copy.deepcopy(DEFAULT_CONFIG.get("km_regras", {}))
    if isinstance(cfg, dict):
        for chave in list(base.keys()):
            if chave in cfg:
                base[chave] = cfg.get(chave)

    base["limpar_tokens_exatos"] = _normalizar_lista_textos_norm(base.get("limpar_tokens_exatos", []))

    regex_cfg = base.get("limpar_regex_norm", [])
    if isinstance(regex_cfg, str):
        regex_cfg = [regex_cfg]
    if not isinstance(regex_cfg, list):
        regex_cfg = []
    regex_validos = []
    for padrao in regex_cfg:
        p = limpar(padrao)
        if not p:
            continue
        try:
            re.compile(p)
            regex_validos.append(p)
        except re.error:
            print(f"AVISO: regex de KM invalido ignorado: '{p}'")
    base["limpar_regex_norm"] = regex_validos

    palavra_uber_cfg = base.get("palavra_uber", "")
    base["palavras_uber"] = _normalizar_lista_textos_norm(palavra_uber_cfg)
    if not base["palavras_uber"]:
        base["palavras_uber"] = ["uber"]

    base["remover_asterisco"] = bool(base.get("remover_asterisco", True))
    base["manter_apenas_digitos"] = bool(base.get("manter_apenas_digitos", True))
    base["limpar_quando_uber"] = bool(base.get("limpar_quando_uber", True))
    base["mascarar_repetido_mesmo_dia"] = bool(base.get("mascarar_repetido_mesmo_dia", True))

    incremento = base.get("incremento_km_final_ausente", 10)
    try:
        incremento = int(incremento)
    except (TypeError, ValueError):
        incremento = 10
    if incremento < 0:
        incremento = 0
    base["incremento_km_final_ausente"] = incremento
    return base


def _normalizar_endereco_regras(cfg):
    base = copy.deepcopy(DEFAULT_CONFIG.get("endereco_regras", {}))
    if isinstance(cfg, dict):
        for chave in list(base.keys()):
            if chave in cfg:
                base[chave] = cfg.get(chave)

    mascara = limpar(base.get("mascara_valor", "-"))
    if not mascara:
        mascara = "-"
    base["mascara_valor"] = mascara
    base["mascarar_repetido_mesmo_dia"] = bool(base.get("mascarar_repetido_mesmo_dia", True))
    base["mascarar_iguais_no_registro"] = bool(base.get("mascarar_iguais_no_registro", True))
    return base


def _normalizar_qualidade_regras(cfg):
    base = copy.deepcopy(DEFAULT_CONFIG.get("qualidade_regras", {}))
    if isinstance(cfg, dict):
        for chave in list(base.keys()):
            if chave in cfg:
                base[chave] = cfg.get(chave)

    base["nome_regra_log"] = limpar(base.get("nome_regra_log", "VALIDACAO QUALIDADE")).upper()
    base["prefixo_inconsistente"] = limpar(base.get("prefixo_inconsistente", "INCONSISTENTE:")).upper()
    base["validar_data"] = bool(base.get("validar_data", True))
    base["validar_chamado"] = bool(base.get("validar_chamado", True))
    base["validar_tecnico_vazio"] = bool(base.get("validar_tecnico_vazio", True))
    base["validar_tecnico_mapeado"] = bool(base.get("validar_tecnico_mapeado", True))

    regex_chamado = limpar(base.get("regex_chamado", r"^\d{1,20}$"))
    try:
        re.compile(regex_chamado)
    except re.error:
        regex_chamado = r"^\d{1,20}$"
    base["regex_chamado"] = regex_chamado

    base["motivo_data_invalida"] = limpar(base.get("motivo_data_invalida", "DATA INVALIDA")).upper()
    base["motivo_chamado_invalido"] = limpar(
        base.get("motivo_chamado_invalido", "CHAMADO VAZIO OU INVALIDO")
    ).upper()
    base["motivo_tecnico_vazio"] = limpar(base.get("motivo_tecnico_vazio", "TECNICO VAZIO")).upper()
    base["motivo_tecnico_nao_mapeado"] = limpar(
        base.get("motivo_tecnico_nao_mapeado", "TECNICO NAO MAPEADO")
    ).upper()
    return base


def _normalizar_filtros_regras(cfg):
    base = copy.deepcopy(DEFAULT_CONFIG.get("filtros_regras", {}))
    if isinstance(cfg, dict):
        for chave in list(base.keys()):
            if chave in cfg:
                base[chave] = cfg.get(chave)

    base["somente_inconsistencias_true_values"] = _normalizar_lista_textos_norm(
        base.get("somente_inconsistencias_true_values", [])
    )
    if not base["somente_inconsistencias_true_values"]:
        base["somente_inconsistencias_true_values"] = ["1", "true", "sim", "s", "yes", "y"]

    base["usar_contains_tecnico"] = bool(base.get("usar_contains_tecnico", True))
    base["usar_contains_status"] = bool(base.get("usar_contains_status", True))
    base["usar_contains_cidade"] = bool(base.get("usar_contains_cidade", True))
    base["ignorar_logs_dedup_com_filtro_status_cidade"] = bool(
        base.get("ignorar_logs_dedup_com_filtro_status_cidade", True)
    )
    return base


# -------------------------
# BLOCO D: CARGA DE REGRAS EXTERNAS
# -------------------------
# Faz merge entre defaults e regras externas (arquivo unico e multi-arquivo),
# sem quebrar fallback.
def _ler_json_arquivo(path, descricao):
    if not os.path.exists(path):
        return None

    ultimo_erro = None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception as e:
            ultimo_erro = e

    if ultimo_erro is not None:
        print(f"AVISO: falha ao ler {descricao} '{path}'. Erro: {ultimo_erro}")
    return None


def _aplicar_config_externa(cfg, externo):
    if not isinstance(externo, dict):
        return

    if "tecnicos_regras" in externo and isinstance(externo["tecnicos_regras"], list):
        cfg["tecnicos_regras"] = externo["tecnicos_regras"]

    if "categoria_ordem" in externo and isinstance(externo["categoria_ordem"], list):
        cfg["categoria_ordem"] = [str(x).upper() for x in externo["categoria_ordem"] if limpar(x)]

    if "categoria_palavras_chave" in externo and isinstance(externo["categoria_palavras_chave"], dict):
        cfg["categoria_palavras_chave"] = externo["categoria_palavras_chave"]

    if "categoria_padrao" in externo and limpar(externo["categoria_padrao"]):
        cfg["categoria_padrao"] = str(externo["categoria_padrao"]).upper()

    if "status_regras" in externo and isinstance(externo["status_regras"], dict):
        cfg["status_regras"] = externo["status_regras"]

    if "km_regras" in externo and isinstance(externo["km_regras"], dict):
        cfg["km_regras"] = externo["km_regras"]

    if "endereco_regras" in externo and isinstance(externo["endereco_regras"], dict):
        cfg["endereco_regras"] = externo["endereco_regras"]

    if "qualidade_regras" in externo and isinstance(externo["qualidade_regras"], dict):
        cfg["qualidade_regras"] = externo["qualidade_regras"]

    if "filtros_regras" in externo and isinstance(externo["filtros_regras"], dict):
        cfg["filtros_regras"] = externo["filtros_regras"]


def carregar_config_regras_multiarquivo():
    externo = {}
    tem_dados = False

    tecnicos = _ler_json_arquivo(CONFIG_TECNICOS_PATH, "regras tecnicos")
    if tecnicos is not None:
        if isinstance(tecnicos, list):
            externo["tecnicos_regras"] = tecnicos
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_TECNICOS_PATH}' invalido "
                "(esperado: lista de tecnicos). Ignorando."
            )

    categorias = _ler_json_arquivo(CONFIG_CATEGORIAS_PATH, "regras categorias")
    if categorias is not None:
        if isinstance(categorias, dict):
            if "categoria_ordem" in categorias:
                externo["categoria_ordem"] = categorias.get("categoria_ordem")
                tem_dados = True
            if "categoria_palavras_chave" in categorias:
                externo["categoria_palavras_chave"] = categorias.get("categoria_palavras_chave")
                tem_dados = True
            if "categoria_padrao" in categorias:
                externo["categoria_padrao"] = categorias.get("categoria_padrao")
                tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_CATEGORIAS_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    status = _ler_json_arquivo(CONFIG_STATUS_PATH, "regras status")
    if status is not None:
        if isinstance(status, dict):
            if isinstance(status.get("status_regras"), dict):
                externo["status_regras"] = status.get("status_regras")
            else:
                externo["status_regras"] = status
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_STATUS_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    km = _ler_json_arquivo(CONFIG_KM_PATH, "regras km")
    if km is not None:
        if isinstance(km, dict):
            if isinstance(km.get("km_regras"), dict):
                externo["km_regras"] = km.get("km_regras")
            else:
                externo["km_regras"] = km
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_KM_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    endereco = _ler_json_arquivo(CONFIG_ENDERECO_PATH, "regras endereco")
    if endereco is not None:
        if isinstance(endereco, dict):
            if isinstance(endereco.get("endereco_regras"), dict):
                externo["endereco_regras"] = endereco.get("endereco_regras")
            else:
                externo["endereco_regras"] = endereco
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_ENDERECO_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    qualidade = _ler_json_arquivo(CONFIG_QUALIDADE_PATH, "regras qualidade")
    if qualidade is not None:
        if isinstance(qualidade, dict):
            if isinstance(qualidade.get("qualidade_regras"), dict):
                externo["qualidade_regras"] = qualidade.get("qualidade_regras")
            else:
                externo["qualidade_regras"] = qualidade
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_QUALIDADE_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    filtros = _ler_json_arquivo(CONFIG_FILTROS_PATH, "regras filtros")
    if filtros is not None:
        if isinstance(filtros, dict):
            if isinstance(filtros.get("filtros_regras"), dict):
                externo["filtros_regras"] = filtros.get("filtros_regras")
            else:
                externo["filtros_regras"] = filtros
            tem_dados = True
        else:
            print(
                f"AVISO: '{CONFIG_FILTROS_PATH}' invalido "
                "(esperado: objeto JSON). Ignorando."
            )

    if not tem_dados:
        return None
    return externo


def carregar_config_regras():
    cfg = copy.deepcopy(DEFAULT_CONFIG)

    # Ordem de precedencia:
    # 1) default interno
    # 2) regras_config.json (legado)
    # 3) arquivos em ./regras/ (mais especifico e modular)
    externo_unico = _ler_json_arquivo(CONFIG_PATH, "arquivo de regras")
    if externo_unico is not None:
        if isinstance(externo_unico, dict):
            _aplicar_config_externa(cfg, externo_unico)
        else:
            print(f"AVISO: '{CONFIG_PATH}' invalido (raiz nao e objeto JSON). Ignorando.")

    externo_multi = carregar_config_regras_multiarquivo()
    if externo_multi is not None:
        _aplicar_config_externa(cfg, externo_multi)

    return cfg


def _atualizar_cache_regras(cfg):
    global CONFIG_REGRAS
    global TECNICOS_REGRAS_PADRAO
    global CATEGORIA_ORDEM
    global CATEGORIA_PALAVRAS
    global CATEGORIA_PADRAO
    global STATUS_REGRAS
    global KM_REGRAS
    global ENDERECO_REGRAS
    global QUALIDADE_REGRAS
    global FILTROS_REGRAS

    CONFIG_REGRAS = cfg
    TECNICOS_REGRAS_PADRAO = _normalizar_tecnicos_regras(CONFIG_REGRAS.get("tecnicos_regras", []))
    CATEGORIA_ORDEM = [str(x).upper() for x in CONFIG_REGRAS.get("categoria_ordem", []) if limpar(x)]
    CATEGORIA_PALAVRAS = _normalizar_categoria_palavras(CONFIG_REGRAS)
    CATEGORIA_PADRAO = str(CONFIG_REGRAS.get("categoria_padrao", "PERIFERICO")).upper()
    STATUS_REGRAS = _normalizar_status_regras(CONFIG_REGRAS.get("status_regras", {}))
    KM_REGRAS = _normalizar_km_regras(CONFIG_REGRAS.get("km_regras", {}))
    ENDERECO_REGRAS = _normalizar_endereco_regras(CONFIG_REGRAS.get("endereco_regras", {}))
    QUALIDADE_REGRAS = _normalizar_qualidade_regras(CONFIG_REGRAS.get("qualidade_regras", {}))
    FILTROS_REGRAS = _normalizar_filtros_regras(CONFIG_REGRAS.get("filtros_regras", {}))

    if not CATEGORIA_ORDEM:
        CATEGORIA_ORDEM = ["IMPRESSORA", "NOBREAK", "NOTEBOOK", "DESKTOP", "PERIFERICO"]


def recarregar_regras():
    cfg = carregar_config_regras()
    _atualizar_cache_regras(cfg)
    return {
        "tecnicos_regras": len(TECNICOS_REGRAS_PADRAO),
        "categoria_ordem": len(CATEGORIA_ORDEM),
        "categorias_palavras": sum(len(v) for v in CATEGORIA_PALAVRAS.values()),
        "arquivo_legado": os.path.exists(CONFIG_PATH),
        "arquivo_tecnicos": os.path.exists(CONFIG_TECNICOS_PATH),
        "arquivo_categorias": os.path.exists(CONFIG_CATEGORIAS_PATH),
        "arquivo_status": os.path.exists(CONFIG_STATUS_PATH),
        "arquivo_km": os.path.exists(CONFIG_KM_PATH),
        "arquivo_endereco": os.path.exists(CONFIG_ENDERECO_PATH),
        "arquivo_qualidade": os.path.exists(CONFIG_QUALIDADE_PATH),
        "arquivo_filtros": os.path.exists(CONFIG_FILTROS_PATH),
    }


_atualizar_cache_regras(carregar_config_regras())


# -------------------------
# BLOCO E: LIMPEZA DE LINHAS E CONVERSAO DE DATA/HORA
# -------------------------
# Trata prefixos do WhatsApp e normaliza formatos de data/hora.
def normalizar_rotulo(rotulo):
    r = norm(rotulo)
    r = re.sub(r"[^a-z0-9]+", " ", r)
    return re.sub(r"\s+", " ", r).strip()


def remover_prefixo_whatsapp(linha):
    if linha is None:
        return ""
    return WHATSAPP_PREFIXO_RE.sub("", str(linha), count=1).strip()


def limpar_linha(linha):
    linha = remover_prefixo_whatsapp(linha).strip()
    if not linha:
        return ""
    baixa = norm(linha)
    if any(
        termo in baixa
        for termo in [
            "<midia",
            "midia oculta",
            "mensagem apagada",
            "localizacao em tempo real compartilhada",
            "audio omitida",
            "omitida",
        ]
    ):
        return ""
    return linha


def converter_data(data_txt):
    data_txt = limpar(data_txt)
    if not data_txt:
        return ""

    candidatos = [data_txt]
    candidatos.extend(re.findall(r"\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4}", data_txt))
    candidatos.extend(re.findall(r"\d{4}[\/\.-]\d{1,2}[\/\.-]\d{1,2}", data_txt))
    candidatos.extend(re.findall(r"\b\d{8}\b", data_txt))

    for c in candidatos:
        c = limpar(c)
        if re.fullmatch(r"\d{8}", c):
            for formato in ("%d%m%Y", "%d%m%y", "%Y%m%d"):
                try:
                    return datetime.strptime(c, formato).strftime("%d/%m/%Y")
                except ValueError:
                    continue

        for formato in (
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%m-%Y",
            "%d-%m-%y",
            "%d.%m.%Y",
            "%d.%m.%y",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y.%m.%d",
        ):
            try:
                return datetime.strptime(c, formato).strftime("%d/%m/%Y")
            except ValueError:
                continue
    return data_txt


def data_para_date(data_txt):
    d = converter_data(data_txt)
    if not d:
        return None
    try:
        return datetime.strptime(d, "%d/%m/%Y").date()
    except ValueError:
        return None


def normalizar_data_filtro(data_txt, nome_campo):
    txt = limpar(data_txt)
    if not txt:
        return None
    d = data_para_date(txt)
    if d is None:
        raise ValueError(
            f"{nome_campo} inválida: '{data_txt}'. Use formatos como dd/mm/aaaa."
        )
    return d


def normalizar_hora(valor):
    v = limpar(valor)
    if not v:
        return ""

    # padroes comuns: "17;05" / "17.05"
    v = v.replace(";", ":").replace(".", ":")
    v = re.sub(r"\s*:\s*", ":", v)
    v = re.sub(r":{2,}", ":", v)

    # padrao HH:MM / HH:M
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]?\d)\b", v)
    if m:
        minuto = int(m.group(2))
        if 0 <= minuto <= 59:
            return f"{int(m.group(1)):02d}:{minuto:02d}"

    # padrao "10h" ou "10h30"
    m = re.search(r"\b([01]?\d|2[0-3])\s*h\s*([0-5]?\d)?\b", norm(v))
    if m:
        hora = int(m.group(1))
        minuto_txt = m.group(2) if m.group(2) is not None else "00"
        minuto = int(minuto_txt)
        if 0 <= minuto <= 59:
            return f"{hora:02d}:{minuto:02d}"

    # padrao "1125" / "930"
    m = re.search(r"\b(\d{3,4})\b", re.sub(r"\D", " ", v))
    if m:
        bruto = m.group(1)
        if len(bruto) == 3:
            hora = int(bruto[0])
            minuto = int(bruto[1:])
        else:
            hora = int(bruto[:2])
            minuto = int(bruto[2:])
        if 0 <= hora <= 23 and 0 <= minuto <= 59:
            return f"{hora:02d}:{minuto:02d}"

    return ""


# -------------------------
# BLOCO F: REGRAS DE TECNICO E CATEGORIA
# -------------------------
# Resolve estado/cidade/endereco-base e categoria de equipamento.
def regra_tecnico(nome, tecnicos_regras=None):
    n = norm(nome)
    regras_ativas = TECNICOS_REGRAS_PADRAO if tecnicos_regras is None else tecnicos_regras
    for regra in regras_ativas:
        if regra["match_norm"] and regra["match_norm"] in n:
            return regra
    return None


def mapear_tecnico_saida(nome, tecnicos_regras=None):
    regra = regra_tecnico(nome, tecnicos_regras=tecnicos_regras)
    if regra and regra.get("tecnico_saida"):
        return regra["tecnico_saida"]
    return nome


def detectar_tecnico(nome, tecnicos_regras=None):
    regra = regra_tecnico(nome, tecnicos_regras=tecnicos_regras)
    if not regra:
        return "", ""
    return regra.get("estado", ""), regra.get("cidade", "")


def base_tecnico(nome, tecnicos_regras=None):
    regra = regra_tecnico(nome, tecnicos_regras=tecnicos_regras)
    if not regra:
        return ""
    return regra.get("endereco_partida", "")


def expediente_tecnico(nome, tecnicos_regras=None):
    regra = regra_tecnico(nome, tecnicos_regras=tecnicos_regras)
    if not regra:
        return EXPEDIENTE_INICIO_PADRAO, EXPEDIENTE_FIM_PADRAO
    inicio = normalizar_hhmm_basico(
        regra.get("horario_inicio_expediente", ""),
        EXPEDIENTE_INICIO_PADRAO,
    )
    fim = normalizar_hhmm_basico(
        regra.get("horario_fim_expediente", ""),
        EXPEDIENTE_FIM_PADRAO,
    )
    return inicio, fim


def categoria_fixa_tecnico(nome, tecnicos_regras=None):
    regra = regra_tecnico(nome, tecnicos_regras=tecnicos_regras)
    if not regra:
        return ""
    return limpar(regra.get("categoria_fixa", ""))


def categoria(texto):
    t = norm(texto)
    if not t:
        return CATEGORIA_PADRAO

    for categoria_nome in CATEGORIA_ORDEM:
        palavras = CATEGORIA_PALAVRAS.get(categoria_nome, [])
        if not palavras:
            continue
        for palavra in palavras:
            if palavra == "pc":
                if re.search(r"\bpc\b", t):
                    return categoria_nome
            elif palavra in t:
                return categoria_nome
    return CATEGORIA_PADRAO


def contem_palavra_config(texto_normalizado, palavras):
    return any(p and p in texto_normalizado for p in (palavras or []))


def classificar_status_descricao(status_original, atividade_realizada):
    status_norm = norm(status_original)
    if not status_norm:
        # fallback para blocos sem rotulo de status explicito.
        status_norm = norm(atividade_realizada)

    palavras_improdutivo = STATUS_REGRAS.get("palavras_improdutivo", [])
    palavras_avaliacao = STATUS_REGRAS.get("palavras_avaliacao", [])

    if contem_palavra_config(status_norm, palavras_improdutivo):
        return (
            STATUS_REGRAS.get("status_improdutivo", "IMPRODUTIVO"),
            STATUS_REGRAS.get("descricao_improdutivo", "IMPRODUTIVO"),
        )
    if contem_palavra_config(status_norm, palavras_avaliacao):
        return (
            STATUS_REGRAS.get("status_padrao", "RESOLVIDO"),
            STATUS_REGRAS.get("descricao_avaliacao", "AVALIAÇÃO"),
        )
    return (
        STATUS_REGRAS.get("status_padrao", "RESOLVIDO"),
        STATUS_REGRAS.get("descricao_padrao", "MANUTENÇÃO"),
    )


def km_texto_contem_uber(*valores):
    palavras_uber = KM_REGRAS.get("palavras_uber", ["uber"])
    for valor in valores:
        n = norm(valor)
        if not n:
            continue
        if contem_palavra_config(n, palavras_uber):
            return True
    return False


def hora_hhmm_para_minutos(valor):
    h = normalizar_hora(valor)
    if not h:
        return None
    m = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", h)
    if not m:
        return None
    return (int(m.group(1)) * 60) + int(m.group(2))


def minutos_para_hora_hhmm(minutos):
    if minutos is None:
        return ""
    total = int(minutos) % (24 * 60)
    h = total // 60
    m = total % 60
    return f"{h:02d}:{m:02d}"


def calcular_termino_retorno(termino_ultimo_chamado, horario_fim_expediente):
    termino_min = hora_hhmm_para_minutos(termino_ultimo_chamado)
    fim_exp_min = hora_hhmm_para_minutos(horario_fim_expediente)
    if termino_min is None and fim_exp_min is None:
        return ""
    if termino_min is None:
        return minutos_para_hora_hhmm(fim_exp_min)
    if fim_exp_min is None:
        return minutos_para_hora_hhmm(termino_min + 1)
    if termino_min < fim_exp_min:
        return minutos_para_hora_hhmm(fim_exp_min)
    return minutos_para_hora_hhmm(termino_min + 1)


def _http_get_json(url):
    req = Request(
        url,
        headers={
            "User-Agent": "extrator-rats/1.0 (python)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=ROTA_TIMEOUT_SEGUNDOS) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extrair_coord_nominatim(dados):
    if not isinstance(dados, list) or not dados:
        return None
    lat = float(dados[0].get("lat"))
    lon = float(dados[0].get("lon"))
    return (lat, lon)


def _extrair_coord_photon(dados):
    if not isinstance(dados, dict):
        return None
    features = dados.get("features", [])
    if not isinstance(features, list) or not features:
        return None
    geometry = features[0].get("geometry", {})
    coords = geometry.get("coordinates", [])
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None
    lon = float(coords[0])
    lat = float(coords[1])
    return (lat, lon)


def normalizar_endereco_para_rota(endereco):
    txt = limpar(endereco)
    if not txt:
        return ""
    txt = unicodedata.normalize("NFD", txt)
    txt = txt.encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^A-Za-z0-9,./\- ]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def calcular_distancia_km_linha_reta(coord_origem, coord_destino):
    if coord_origem is None or coord_destino is None:
        return None
    try:
        lat1, lon1 = coord_origem
        lat2, lon2 = coord_destino
        raio_terra_km = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return max(raio_terra_km * c, 0.0)
    except Exception:
        return None


def geocodificar_endereco(endereco):
    global _ROTA_TENTATIVAS

    chave = norm(endereco)
    if not chave:
        return None
    if chave in _CACHE_GEO:
        return _CACHE_GEO[chave]
    if _ROTA_TENTATIVAS >= ROTA_MAX_TENTATIVAS:
        _CACHE_GEO[chave] = None
        return None

    endereco_consulta = normalizar_endereco_para_rota(endereco)
    if not endereco_consulta:
        _CACHE_GEO[chave] = None
        return None

    consultas = [
        (
            "https://nominatim.openstreetmap.org/search?"
            + urlencode(
                {
                    "q": endereco_consulta,
                    "format": "json",
                    "limit": 1,
                }
            ),
            _extrair_coord_nominatim,
        ),
        (
            "https://photon.komoot.io/api/?"
            + urlencode(
                {
                    "q": endereco_consulta,
                    "limit": 1,
                }
            ),
            _extrair_coord_photon,
        ),
    ]

    for url, extrator in consultas:
        if _ROTA_TENTATIVAS >= ROTA_MAX_TENTATIVAS:
            break
        try:
            _ROTA_TENTATIVAS += 1
            dados = _http_get_json(url)
            coord = extrator(dados)
            if coord is not None:
                _CACHE_GEO[chave] = coord
                return coord
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, json.JSONDecodeError):
            continue
        except Exception:
            continue

    _CACHE_GEO[chave] = None
    return None


def consultar_km_rota(origem, destino):
    global _ROTA_TENTATIVAS

    chave = (norm(origem), norm(destino))
    if chave in _CACHE_ROTA:
        return _CACHE_ROTA[chave]
    if _ROTA_TENTATIVAS >= ROTA_MAX_TENTATIVAS:
        _CACHE_ROTA[chave] = None
        return None

    if not chave[0] or not chave[1]:
        _CACHE_ROTA[chave] = None
        return None
    if chave[0] == chave[1]:
        _CACHE_ROTA[chave] = 0
        return 0

    coord_origem = geocodificar_endereco(origem)
    coord_destino = geocodificar_endereco(destino)
    if coord_origem is None or coord_destino is None:
        _CACHE_ROTA[chave] = None
        return None
    km_linha_reta = calcular_distancia_km_linha_reta(coord_origem, coord_destino)

    try:
        lat1, lon1 = coord_origem
        lat2, lon2 = coord_destino
        _ROTA_TENTATIVAS += 1
        url = (
            "https://router.project-osrm.org/route/v1/driving/"
            f"{quote(str(lon1))},{quote(str(lat1))};{quote(str(lon2))},{quote(str(lat2))}"
            "?overview=false"
        )
        dados = _http_get_json(url)
        rotas = dados.get("routes", [])
        if not rotas:
            if km_linha_reta is not None:
                km_aprox = max(int(math.ceil(km_linha_reta)), 0)
                _CACHE_ROTA[chave] = km_aprox
                return km_aprox
            _CACHE_ROTA[chave] = None
            return None
        distancia_m = float(rotas[0].get("distance", 0))
        km = max(int(math.ceil(distancia_m / 1000.0)), 0)
        _CACHE_ROTA[chave] = km
        return km
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError, json.JSONDecodeError):
        if km_linha_reta is not None:
            km_aprox = max(int(math.ceil(km_linha_reta)), 0)
            _CACHE_ROTA[chave] = km_aprox
            return km_aprox
        _CACHE_ROTA[chave] = None
        return None
    except Exception:
        if km_linha_reta is not None:
            km_aprox = max(int(math.ceil(km_linha_reta)), 0)
            _CACHE_ROTA[chave] = km_aprox
            return km_aprox
        _CACHE_ROTA[chave] = None
        return None


# -------------------------
# BLOCO G: IDENTIFICACAO DE BLOCOS RAT E CAMPOS
# -------------------------
# Reconhece inicio de cada RAT e mapeia rotulos variados para campos canonicos.
def cabecalho_data_valido(linha):
    txt = remover_prefixo_whatsapp("" if linha is None else str(linha)).strip()
    txt = txt.strip("* ").strip()

    m = re.match(r"^data(?:\s+do\s+atendimento)?\s*:\s*(.+)$", norm(txt), flags=re.IGNORECASE)
    if not m:
        return False

    valor = limpar(m.group(1))
    if not valor:
        return False

    return (
        re.search(r"\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4}", valor) is not None
        or re.search(r"\b\d{8}\b", valor) is not None
    )


def eh_inicio_rat(linha):
    linha_txt = "" if linha is None else str(linha)
    n = norm(remover_prefixo_whatsapp(linha_txt))

    # "DATA DO ATENDIMENTO" só é início quando vem em linha de mensagem
    # exportada do WhatsApp (padrão antigo).
    if "data do atendimento" in n and WHATSAPP_PREFIXO_RE.search(linha_txt):
        return True

    return (
        "script de fechamento" in n
        or re.search(r"\btecnico\s*:", n) is not None
        or re.search(r"\bnome completo do tecnico\s*:", n) is not None
        or re.search(r"\bnome do tecnico\s*:", n) is not None
    )


def extrair_rats(linhas):
    rats = []
    atual = []

    for linha in linhas:
        linha_bruta = "" if linha is None else str(linha).strip()
        limpa = limpar_linha(linha_bruta)
        if not limpa:
            continue

        if eh_inicio_rat(linha_bruta) and len(atual) > 3:
            rats.append(atual)
            atual = []

        atual.append(linha_bruta)

    if atual:
        rats.append(atual)

    return rats


def dividir_rotulo_valor(linha):
    l = limpar(linha)
    if not l:
        return "", ""

    # Formato: "*Rótulo: valor*"
    m = re.match(r"^\s*\*+\s*([^:*]{1,120}?)\s*:\s*(.*?)\s*\*+\s*$", l)
    if m:
        rotulo = limpar(m.group(1)).strip("* ").strip()
        valor = limpar(m.group(2))
        return rotulo, valor

    # Primeiro tenta o formato com asteriscos para evitar quebrar em "12:45".
    m = re.match(r"^\s*\*+\s*([^*]{1,120}?)\s*:?\s*\*+\s*(.*)$", l)
    if not m:
        m = re.match(r"^\s*([^:]{1,120})\s*:\s*(.*)$", l)
        if not m:
            return "", ""

    rotulo = limpar(m.group(1)).strip("* ").strip()
    valor = limpar(m.group(2))
    return rotulo, valor


ROTULOS_POSSIVEIS_EM_VALOR_RE = re.compile(
    r"^\s*(?:\*+\s*)?(?:"
    r"DATA(?:\s+DO\s+ATENDIMENTO)?|"
    r"CHAMADO|"
    r"CLIENTE|"
    r"T[ÉE]CNICO|"
    r"NOME\s+COMPLETO\s+DO\s+T[ÉE]CNICO|"
    r"ENDERE[CÇ]O|"
    r"KM\s+INICIAL|"
    r"KM\s+FINAL|"
    r"PREVIS[ÃA]O\s+DE\s+CHEGADA(?:\s+NO\s+CLIENTE)?|"
    r"HOR[ÁA]RIO\s+DE\s+IN[ÍI]CIO\s+DA\s+ATIVIDADE|"
    r"HOR[ÁA]RIO\s+DE\s+T[ÉE]RMINO\s+(?:DA|DE)\s+ATIVIDADE|"
    r"ATIVIDADE\s+REALIZADA|"
    r"STATUS\s+DO\s+CHAMADO|"
    r"NOME\s+DE\s+QUEM\s+ACOMPANHOU\s+A\s+ATIVIDADE"
    r")\s*:\s*",
    flags=re.IGNORECASE,
)


def limpar_rotulos_repetidos_no_inicio(valor, max_iter=4):
    txt = limpar(valor).strip("* ").strip()
    if not txt:
        return ""
    for _ in range(max_iter):
        novo = ROTULOS_POSSIVEIS_EM_VALOR_RE.sub("", txt, count=1).strip("* ").strip()
        if novo == txt:
            break
        txt = novo
    return txt


def separar_status_e_acompanhou(valor):
    v = limpar(valor)
    if not v:
        return "", ""

    m = re.search(
        r"\b(?:quem\s+acompanhou\s+a?\s*atividades?|acompanhou\s+a?\s*atividades?)\b\s*:?\s*(.*)$",
        v,
        flags=re.IGNORECASE,
    )
    if not m:
        return v, ""

    status_txt = limpar(v[: m.start()])
    acompanhou_txt = limpar(m.group(1))
    return status_txt, acompanhou_txt


def _limpar_candidato_acompanhou(valor):
    txt = limpar(valor).strip("* ").strip(" -:;,.")
    txt = re.split(
        r"\b(?:durante|no|na|nos|nas|para|em|devido|com|apos|após|quando|onde)\b",
        txt,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip()
    if txt:
        partes = txt.split()
        if len(partes) > 4:
            txt = " ".join(partes[:4])
    if not txt:
        return ""
    n = norm(txt)
    if n in {
        "",
        "-",
        "*",
        "nao informado",
        "na",
        "n/a",
        "x",
        "xx",
        "xxx",
        "xxxx",
        "xxxxx",
        "nenhum",
        "sem acompanhamento",
    }:
        return ""
    if len(txt) > 80:
        return ""
    if re.search(r"\d{1,2}:\d{1,2}", txt):
        return ""
    return txt


def inferir_quem_acompanhou(valor_atual, atividade_realizada, status_original=""):
    atual = _limpar_candidato_acompanhou(valor_atual)
    if atual:
        return atual

    texto = limpar(f"{atividade_realizada} {status_original}")
    if not texto:
        return ""
    texto_norm = norm(texto)

    sem_acompanhamento = [
        "sem acompanhamento",
        "sem acompanhante",
        "nao havia ninguem",
        "nao tinha ninguem",
        "nenhum responsavel",
        "nao havia responsavel",
        "sem responsavel",
        "ninguem para acompanhar",
        "ninguem para acompanhar",
    ]
    if any(t in texto_norm for t in sem_acompanhamento):
        return ""

    padroes_nome = [
        r"(?:acompanhad[oa]\s+por|com\s+acompanhamento\s+de|quem\s+acompanhou(?:\s+a?\s*atividade)?\s*[:\-]?)\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'`\-]+(?:\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'`\-]+){0,4})",
        r"(?:respons[aá]vel(?:\s+local)?|ti\s+local|time\s+de\s+t\.?i)\s*[:\-]\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'`\-]+(?:\s+[A-Za-zÀ-ÿ][A-Za-zÀ-ÿ'`\-]+){0,4})",
    ]
    candidatos_invalidos = {
        "ti",
        "ti local",
        "local",
        "responsavel",
        "responsavel local",
        "cliente",
        "usuario",
        "colaborador",
        "suporte",
    }
    for padrao in padroes_nome:
        m = re.search(padrao, texto, flags=re.IGNORECASE)
        if not m:
            continue
        candidato = _limpar_candidato_acompanhou(m.group(1))
        if not candidato:
            continue
        if norm(candidato) in candidatos_invalidos:
            continue
        return candidato

    if "ti local" in texto_norm or "time de ti" in texto_norm or "time de t i" in texto_norm:
        return "TI LOCAL"
    if "responsavel local" in texto_norm or "responsavel do local" in texto_norm:
        return "RESPONSÁVEL LOCAL"
    if "acompanhado pelo cliente" in texto_norm or "acompanhado por cliente" in texto_norm:
        return "CLIENTE"

    return ""


def atividade_realizada_semantica_valida(valor):
    txt = limpar(valor).strip("* ").strip()
    if not txt:
        return False

    n = normalizar_rotulo(txt)
    if not n or n in {"-", "nao informado", "n a", "na"}:
        return False

    # Evita considerar como atividade linhas que sao, na pratica, outros rotulos.
    prefixos_rotulo_nao_atividade = [
        "numero de patrimonio",
        "numero patrimonio",
        "n patrimonio",
        "n o patrimonio",
        "n o patrimonio serial",
        "status do chamado",
        "nome de quem acompanhou",
        "quem acompanhou atividade",
        "quem acompanhou a atividade",
        "tipo de equipamento",
        "tipo do equipamento",
        "modelo do equipamento",
        "parceiro",
    ]
    if any(n.startswith(p) for p in prefixos_rotulo_nao_atividade):
        return False

    if re.fullmatch(r"[-*/\d\s]{1,20}", txt):
        return False

    return True


def rotulo_canonico(rotulo):
    r = normalizar_rotulo(rotulo)
    if not r:
        return ""

    if (
        "nome completo do tecnico" in r
        or "nome do tecnico" in r
        or r.startswith("tecnico")
        or r.startswith("etecnico")
    ):
        return "TÉCNICO"
    if "data do atendimento" in r or r == "data":
        return "DATA"
    if r.startswith("cliente"):
        return "CLIENTE"
    if (
        r.startswith("chamado")
        or r.startswith("chamad0")
        or "numero do chamado" in r
        or "n do chamado" in r
    ):
        return "CHAMADO"
    if r.startswith("km inicial"):
        return "KM INICIAL"
    if r.startswith("km final"):
        return "KM FINAL"
    if (
        "previsao de chegada no cliente" in r
        or "previsao de chegada" in r
        or "chegada no cliente" in r
    ):
        return "PREVISAO CHEGADA"
    if r.startswith("parceiro"):
        return "PARCEIRO AUX"
    if (
        "horario de inicio da atividade" in r
        or "horario de inicio de atividade" in r
        or "horario de inicio atividade" in r
        or "horario inicio da atividade" in r
        or "horario inicio atividade" in r
        or r == "inicio da atividade"
    ):
        return "INICIO ATIVIDADE"
    if (
        "horario de termino da atividade" in r
        or "horario de termino de atividade" in r
        or "horario de termino das atividades" in r
        or "horario de termino das atividade" in r
        or "horario termino da atividade" in r
        or r == "termino da atividade"
        or r == "termino das atividades"
    ):
        return "TÉRMINO DA ATIVIDADE"
    if (
        "numero de patrimonio" in r
        or "numero patrimonio" in r
        or "patrimonio serial" in r
        or "patrimonio/serial" in r
        or r.startswith("n patrimonio")
        or r.startswith("n o patrimonio")
        or r.startswith("n patrimonio serial")
    ):
        return "PATRIMÔNIO AUX"
    if r.startswith("endereco"):
        return "ENDEREÇO CLIENTE"
    if r.startswith("obs") or r.startswith("observacao"):
        return "OBS AUX"
    if (
        "atividade realizada" in r
        or "atividades realizadas" in r
        or "descricao das atividades" in r
        or "descricao da atividade" in r
        or r.startswith("atividade")
    ):
        return "ATIVIDADE REALIZADA"
    if (
        "nome de quem acompanhou a atividade" in r
        or "nome de quem acompanhou as atividades" in r
        or "quem acompanhou atividade" in r
        or "quem acompanhou a atividade" in r
        or "quem acompanhou as atividades" in r
        or "acompanhou a atividade" in r
        or "acompanhou a atividades" in r
        or "acompanhou as atividades" in r
        or r.startswith("acompanhou")
    ):
        return "QUEM ACOMPANHOU"
    if (
        "status do chamado" in r
        or "status chamado" in r
        or "status do atendimento" in r
        or "status da atividade" in r
        or "statos do chamado" in r
        or "situacao do chamado" in r
        or r.startswith("status")
        or r.startswith("statos")
    ):
        return "STATUS ORIGINAL"
    if "problema identificado" in r:
        return "PROBLEMA AUX"
    if "tipo do equipamento" in r or "tipo de equipamento" in r:
        return "TIPO AUX"
    if "modelo do equipamento" in r:
        return "MODELO AUX"
    return ""


def extrair_campos(bloco):
    cabecalhos_soltos_invalidos = {
        "inicio",
        "inicio da atividade",
        "termino",
        "termino da atividade",
        "data",
        "chamado",
        "status",
        "status do chamado",
        "atividade realizada",
        "script",
        "script de fechamento",
    }

    def valor_linha_seguinte_valido(canon, candidato):
        txt = limpar(candidato).strip("* ").strip()
        if not txt:
            return False
        n = norm(txt)
        if not n or n in {"-", "*", ":"}:
            return False
        if n in cabecalhos_soltos_invalidos:
            return False
        if canon == "QUEM ACOMPANHOU":
            if n in {"nao informado", "n/a", "na"}:
                return False
            # evita pegar horario/campos tecnicos no lugar de nome.
            if re.fullmatch(r"\d{1,2}:\d{1,2}", txt):
                return False
        return True

    campos = {}
    linhas = [limpar_linha(l) for l in bloco]
    linhas = [l for l in linhas if l]

    i = 0
    while i < len(linhas):
        linha = linhas[i]
        rotulo, valor = dividir_rotulo_valor(linha)
        canon = rotulo_canonico(rotulo)

        if not canon:
            i += 1
            continue

        if canon in CAMPOS_MULTILINHA:
            partes = [valor] if valor else []
            j = i + 1
            while j < len(linhas):
                prox_rotulo, _ = dividir_rotulo_valor(linhas[j])
                if prox_rotulo and rotulo_canonico(prox_rotulo):
                    break
                partes.append(linhas[j])
                j += 1
            valor_final = limpar(" ".join(p for p in partes if p))
            i = j
        else:
            valor_final = limpar(valor).strip("* ").strip()
            i_prox = i + 1
            if not valor_final and canon in CAMPOS_VALOR_LINHA_SEGUINTE:
                j = i + 1
                while j < len(linhas):
                    linha_candidata = limpar(linhas[j])
                    if not linha_candidata:
                        j += 1
                        continue
                    prox_rotulo, _ = dividir_rotulo_valor(linha_candidata)
                    if prox_rotulo and rotulo_canonico(prox_rotulo):
                        break
                    candidato = linha_candidata.strip("* ").strip()
                    if valor_linha_seguinte_valido(canon, candidato):
                        valor_final = candidato
                        i_prox = j + 1
                    break
            i = i_prox

        valor_final = limpar_rotulos_repetidos_no_inicio(valor_final)

        if canon == "STATUS ORIGINAL" and valor_final:
            status_txt, acompanhou_txt = separar_status_e_acompanhou(valor_final)
            if acompanhou_txt and (not campos.get("QUEM ACOMPANHOU")):
                campos["QUEM ACOMPANHOU"] = acompanhou_txt
            valor_final = status_txt or valor_final

        if not valor_final and canon in campos:
            continue

        if canon in campos and campos[canon] and canon in CAMPOS_MULTILINHA and valor_final:
            campos[canon] = limpar(f"{campos[canon]} {valor_final}")
        elif canon not in campos or not campos[canon]:
            campos[canon] = valor_final

    return campos


def extrair_chamado_fallback(bloco):
    encontrados = []
    vistos = set()

    for linha in bloco:
        l = limpar_linha(linha)
        if not l:
            continue

        # Prioridade 1: linha com rótulo reconhecido explicitamente como CHAMADO.
        rotulo, valor = dividir_rotulo_valor(l)
        if rotulo and rotulo_canonico(rotulo) == "CHAMADO":
            for numero in re.findall(r"\b\d{5,10}\b", valor):
                if numero not in vistos:
                    vistos.add(numero)
                    encontrados.append(numero)
            continue

        # Prioridade 2: fallback estrito para linhas iniciadas por "chamado".
        n = norm(l)
        if not re.match(r"^\*?\s*chamado\b", n):
            continue
        for numero in re.findall(r"\b\d{5,10}\b", l):
            if numero not in vistos:
                vistos.add(numero)
                encontrados.append(numero)

    return "/".join(encontrados)


def extrair_atividade_realizada_fallback(bloco):
    linhas = [limpar_linha(l) for l in bloco]
    linhas = [l for l in linhas if l]
    if not linhas:
        return ""

    # Tenta iniciar a captura apos o bloco de horários/deslocamento.
    campos_ancora = {"TÉRMINO DA ATIVIDADE", "INICIO ATIVIDADE", "PREVISAO CHEGADA"}
    idx_inicio = 0
    for idx, linha in enumerate(linhas):
        rotulo, _ = dividir_rotulo_valor(linha)
        canon = rotulo_canonico(rotulo)
        if canon in campos_ancora:
            idx_inicio = idx + 1

    campos_fim = {
        "PATRIMÔNIO AUX",
        "STATUS ORIGINAL",
        "MODELO AUX",
        "TIPO AUX",
        "QUEM ACOMPANHOU",
        "CHAMADO",
        "CLIENTE",
        "DATA",
        "ENDEREÇO CLIENTE",
        "TÉCNICO",
        "PARCEIRO AUX",
        "KM INICIAL",
        "KM FINAL",
        "PREVISAO CHEGADA",
        "INICIO ATIVIDADE",
        "TÉRMINO DA ATIVIDADE",
    }
    padroes_fim_sem_rotulo = [
        r"^(?:usu[áa]rio|usuario)\s*:",
        r"^valida[cç][aã]o\s+por\s+voz\s*:",
        r"^equipamento\s*:",
        r"^script\s+de\s+fechamento\b",
    ]

    partes = []
    for linha in linhas[idx_inicio:]:
        linha_limpa = limpar(linha).strip("* ").strip()
        if not linha_limpa:
            continue

        if WHATSAPP_PREFIXO_RE.search(linha_limpa):
            if partes:
                break
            continue

        rotulo, valor = dividir_rotulo_valor(linha_limpa)
        canon = rotulo_canonico(rotulo)
        if canon:
            if canon in campos_fim:
                if partes:
                    break
                continue
            if canon == "ATIVIDADE REALIZADA":
                valor_limpo = limpar_rotulos_repetidos_no_inicio(valor)
                if valor_limpo:
                    partes.append(valor_limpo)
                continue
            if partes:
                break
            continue

        n = norm(linha_limpa)
        if any(re.match(p, n) for p in padroes_fim_sem_rotulo):
            if partes:
                break
            continue
        if n in {"inicio", "fim"}:
            continue
        if len(re.sub(r"\s+", " ", linha_limpa).strip()) < 5:
            continue

        partes.append(linha_limpa)

    candidato = limpar(" ".join(partes))
    if atividade_realizada_semantica_valida(candidato):
        return candidato
    return ""


# -------------------------
# BLOCO H: PARSE DO RAT E REGRAS DE NEGOCIO POR REGISTRO
# -------------------------
# Monta o registro final, normaliza status/descricao e aplica defaults.
def parse_rat(bloco, tecnicos_regras=None):
    extraido = aplicar_compatibilidade_chaves(extrair_campos(bloco))
    d = {k: "" for k in CAMPOS}

    d["TÉCNICO"] = extraido.get("TÉCNICO", "")
    if not limpar(d["TÉCNICO"]):
        d["TÉCNICO"] = extrair_tecnico_do_prefixo(bloco, tecnicos_regras=tecnicos_regras)
    d["DATA"] = converter_data(extraido.get("DATA", ""))
    d["CLIENTE"] = extraido.get("CLIENTE", "")
    d["CHAMADO"] = extraido.get("CHAMADO", "")
    d["KM INICIAL"] = normalizar_campo_km(extraido.get("KM INICIAL", ""))
    d["KM FINAL"] = normalizar_campo_km(extraido.get("KM FINAL", ""))
    inicio_atividade = normalizar_hora(extraido.get("INICIO ATIVIDADE", ""))
    previsao_chegada = normalizar_hora(extraido.get("PREVISAO CHEGADA", ""))
    d["INICIO DA ATIVIDADE"] = inicio_atividade or previsao_chegada
    d["TÉRMINO DA ATIVIDADE"] = normalizar_hora(extraido.get("TÉRMINO DA ATIVIDADE", ""))
    d["ENDEREÇO CLIENTE"] = extraido.get("ENDEREÇO CLIENTE", "")
    atividade_realizada = extraido.get("ATIVIDADE REALIZADA", "")
    if not atividade_realizada_semantica_valida(atividade_realizada):
        atividade_realizada = extrair_atividade_realizada_fallback(bloco)
    if not atividade_realizada_semantica_valida(atividade_realizada):
        atividade_realizada = ""
    d["ATIVIDADE REALIZADA"] = atividade_realizada
    d["QUEM ACOMPANHOU"] = extraido.get("QUEM ACOMPANHOU", "")
    d["PATRIMÔNIO"] = "1"

    if not limpar(d["CHAMADO"]):
        d["CHAMADO"] = extrair_chamado_fallback(bloco)

    status_original = extraido.get("STATUS ORIGINAL", "")
    d["QUEM ACOMPANHOU"] = inferir_quem_acompanhou(
        d["QUEM ACOMPANHOU"],
        d["ATIVIDADE REALIZADA"],
        status_original=status_original,
    )
    status_normalizado, descricao_normalizada = classificar_status_descricao(
        status_original, d["ATIVIDADE REALIZADA"]
    )
    d["STATUS"] = status_normalizado
    d["DESCRIÇÃO DO CHAMADO"] = descricao_normalizada

    estado, cidade = detectar_tecnico(d["TÉCNICO"], tecnicos_regras=tecnicos_regras)
    d["ESTADO"] = estado
    d["CIDADE"] = cidade

    texto_categoria = " ".join(
        [
            d["ATIVIDADE REALIZADA"],
            extraido.get("TIPO AUX", ""),
            extraido.get("MODELO AUX", ""),
            status_original,
        ]
    )
    categoria_fixa = categoria_fixa_tecnico(d["TÉCNICO"], tecnicos_regras=tecnicos_regras)
    if categoria_fixa:
        d["CATEGORIA"] = categoria_fixa
    else:
        d["CATEGORIA"] = categoria(texto_categoria)

    if not d["DATA"]:
        d["DATA"] = extrair_data_do_prefixo(bloco)

    return d


def extrair_data_do_prefixo(bloco):
    for linha in bloco:
        m = re.match(
            r"^\s*(\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})\s+\d{1,2}:\d{2}\s*-\s*",
            str(linha),
        )
        if m:
            return converter_data(m.group(1))
    return ""


def extrair_tecnico_do_prefixo(bloco, tecnicos_regras=None):
    palavras_bloqueadas = {
        "coordenador",
        "analista",
        "supervisor",
        "equipe fixa",
        "suporte",
        "sustentacao",
    }

    for linha in bloco:
        m = re.match(
            r"^\s*\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4}\s+\d{1,2}:\d{2}\s*-\s*([^:]+):",
            str(linha),
        )
        if not m:
            continue

        nome = limpar(m.group(1))
        if not nome:
            continue

        nome_norm = norm(nome)
        if re.search(r"\+\d{2,}", nome):
            continue

        # Evita pegar remetentes administrativos.
        if any(p in nome_norm for p in palavras_bloqueadas):
            # se tiver indicacao explicita de tecnico/impressora, ainda pode ser valido.
            if "tecnico" not in nome_norm and "impressora" not in nome_norm:
                continue

        # remove sufixos comuns vindos do nome de exibicao no WhatsApp.
        nome_limpo = re.sub(
            r"\b(tecnico|técnico|impressora|impressoras)\b.*$",
            "",
            nome,
            flags=re.IGNORECASE,
        )
        nome_limpo = limpar(nome_limpo)
        if not nome_limpo:
            nome_limpo = nome

        # prioriza remetentes que batem com regras conhecidas.
        if regra_tecnico(nome_limpo, tecnicos_regras=tecnicos_regras) or regra_tecnico(
            nome, tecnicos_regras=tecnicos_regras
        ):
            return nome_limpo

        # fallback restrito: so aceita quando o proprio remetente traz marcador
        # de perfil tecnico/impressora.
        if (
            ("tecnico" in nome_norm or "impressora" in nome_norm)
            and re.fullmatch(r"[A-Za-zÀ-ÿ ]{4,80}", nome_limpo)
            and len(nome_limpo.split()) >= 2
        ):
            return nome_limpo

    return ""


def forcar_maiusculas(registro):
    registro = aplicar_compatibilidade_chaves(dict(registro) if isinstance(registro, dict) else {})
    out = {}
    for campo in CAMPOS:
        valor = limpar(registro.get(campo, ""))
        if not valor and campo in CAMPOS_NAO_INFORMADO:
            registrar_alteracao_linha(
                registro,
                "PREENCHIMENTO PADRÃO",
                campo,
                valor,
                "NÃO INFORMADO",
            )
            valor = "NÃO INFORMADO"
        out[campo] = valor.upper()
    out["_ARQUIVO_ORIGEM"] = registro.get("_ARQUIVO_ORIGEM", "")
    out["_LOGS"] = list(registro.get("_LOGS", []))
    out["_TEM_INCONSISTENCIA"] = bool(registro.get("_TEM_INCONSISTENCIA", False))
    out["_TIPO_REGISTRO"] = registro.get("_TIPO_REGISTRO", "")
    return out


# -------------------------
# BLOCO I: APOIO DE ORDENACAO, COMPARACAO E LOG
# -------------------------
# Funcoes utilitarias para ordenacao por data/hora e trilha de alteracoes.
def data_sort_key(data_txt):
    try:
        return datetime.strptime(data_txt, "%d/%m/%Y").date()
    except ValueError:
        return datetime.max.date()


def hora_sort_key(valor):
    v = limpar(valor)
    if not v:
        return 9999
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", v)
    if m:
        h, minuto = int(m.group(1)), int(m.group(2))
        return (h * 60) + minuto
    m = re.search(r"\b(\d+)\s*min\b", norm(v))
    if m:
        return int(m.group(1))
    return 9999


def extrair_chamados(chamado_txt):
    return re.findall(r"\d+", chamado_txt or "")


def mesmo_valor(a, b):
    a_norm = norm(a)
    b_norm = norm(b)
    return bool(a_norm) and a_norm == b_norm


def valor_informativo(valor):
    v = norm(valor)
    return bool(v) and v not in {"nao informado", "-"}


def pontuacao_registro_para_dedup(linha):
    if not isinstance(linha, dict):
        return (0, 0, 0)

    campos_base = [
        "DATA",
        "CHAMADO",
        "CLIENTE",
        "ESTADO",
        "CIDADE",
        "TÉCNICO",
        "DESCRIÇÃO DO CHAMADO",
        "KM INICIAL",
        "KM FINAL",
        "INICIO DA ATIVIDADE",
        "TÉRMINO DA ATIVIDADE",
        "ENDEREÇO DE PARTIDA",
        "ENDEREÇO CLIENTE",
        "ATIVIDADE REALIZADA",
        "PATRIMÔNIO",
        "CATEGORIA",
        "STATUS",
        "QUEM ACOMPANHOU",
    ]
    preenchidos = sum(1 for c in campos_base if valor_informativo(linha.get(c, "")))
    prioridade = sum(
        1
        for c in [
            "CLIENTE",
            "TÉCNICO",
            "INICIO DA ATIVIDADE",
            "TÉRMINO DA ATIVIDADE",
            "ATIVIDADE REALIZADA",
            "ENDEREÇO CLIENTE",
            "STATUS",
        ]
        if valor_informativo(linha.get(c, ""))
    )
    termino_norm = normalizar_hora(linha.get("TÉRMINO DA ATIVIDADE", ""))
    chegada_norm = normalizar_hora(linha.get("INICIO DA ATIVIDADE", ""))
    score_termino = hora_sort_key(termino_norm) if termino_norm else 0
    score_chegada = hora_sort_key(chegada_norm) if chegada_norm else 0
    tamanho_atividade = len(limpar(linha.get("ATIVIDADE REALIZADA", "")))
    return (preenchidos, prioridade, score_termino, score_chegada, tamanho_atividade)


def normalizar_campo_km(valor):
    v = limpar(valor)
    if not v:
        return ""

    n = norm(v)
    tokens_exatos = set(KM_REGRAS.get("limpar_tokens_exatos", []))
    if n in tokens_exatos:
        return ""

    for padrao in KM_REGRAS.get("limpar_regex_norm", []):
        try:
            if re.fullmatch(padrao, n):
                return ""
        except re.error:
            continue

    if KM_REGRAS.get("limpar_quando_uber", True) and km_texto_contem_uber(v):
        return ""

    # remove marcador inicial com asterisco para casos como "* 6216".
    v_tratado = v
    if KM_REGRAS.get("remover_asterisco", True):
        v_tratado = limpar(v_tratado.replace("*", " "))

    # se tiver dígitos, mantém apenas os números.
    # Ex.: "0.1" -> "01", "* 6216" -> "6216".
    if re.search(r"\d", v_tratado):
        if KM_REGRAS.get("manter_apenas_digitos", True):
            return re.sub(r"\D", "", v_tratado)
        return v_tratado

    return ""


def km_para_int(valor):
    v = normalizar_campo_km(valor)
    if not v:
        return None
    if km_texto_contem_uber(v):
        return None
    if re.fullmatch(r"\d+", v):
        return int(v)
    digitos = re.sub(r"\D", "", v)
    if digitos:
        return int(digitos)
    return None


def calcular_km_percorrido(km_inicial, km_final):
    km_ini_int = km_para_int(km_inicial)
    km_fim_int = km_para_int(km_final)
    if km_ini_int is None or km_fim_int is None:
        return ""
    return str(km_fim_int - km_ini_int)


def pontuacao_info_retorno(info):
    if not isinstance(info, dict):
        return (0, -1, 0, -1, -1, 0, -1)

    km_final_int = km_para_int(info.get("ULTIMO_KM_FINAL", ""))
    km_valido = 1 if km_final_int is not None else 0
    km_score = km_final_int if km_final_int is not None else -1

    inicio_norm = normalizar_hora(info.get("ULTIMO_INICIO", ""))
    termino_norm = normalizar_hora(info.get("ULTIMO_TERMINO", ""))
    qtd_horarios = int(bool(inicio_norm)) + int(bool(termino_norm))
    termino_score = hora_sort_key(termino_norm) if termino_norm else -1
    inicio_score = hora_sort_key(inicio_norm) if inicio_norm else -1

    endereco_score = 1 if valor_informativo(info.get("ULTIMO_ENDERECO_CLIENTE", "")) else 0
    ordem = int(info.get("_ORDEM", -1))

    # Prioridade:
    # 1) KM FINAL válido
    # 2) maior KM FINAL
    # 3) mais horários válidos (inicio/termino)
    # 4) horário de término mais alto
    # 5) horário de início mais alto
    # 6) endereço cliente preenchido
    # 7) ordem de aparição (desempate)
    return (km_valido, km_score, qtd_horarios, termino_score, inicio_score, endereco_score, ordem)


def deve_substituir_info_retorno(atual, candidato):
    if atual is None:
        return True
    return pontuacao_info_retorno(candidato) > pontuacao_info_retorno(atual)


def registrar_alteracao_linha(linha, regra, campo, valor_anterior, valor_final):
    ant = limpar(valor_anterior)
    dep = limpar(valor_final)
    if ant == dep:
        return
    linha.setdefault("_LOGS", []).append(
        {
            "REGRA": regra,
            "CAMPO": campo,
            "VALOR ANTERIOR": ant,
            "VALOR FINAL": dep,
        }
    )


def log_da_linha(linha):
    linha = aplicar_compatibilidade_chaves(dict(linha) if isinstance(linha, dict) else {})
    logs = []
    for ev in linha.get("_LOGS", []):
        logs.append(
            {
                "ARQUIVO ORIGEM": linha.get("_ARQUIVO_ORIGEM", ""),
                "DATA": linha.get("DATA", ""),
                "CHAMADO": linha.get("CHAMADO", ""),
                "TÉCNICO": linha.get("TÉCNICO", ""),
                "REGRA": ev.get("REGRA", ""),
                "CAMPO": ev.get("CAMPO", ""),
                "VALOR ANTERIOR": ev.get("VALOR ANTERIOR", ""),
                "VALOR FINAL": ev.get("VALOR FINAL", ""),
            }
        )
    return logs


def chave_duplicidade(linha):
    linha = aplicar_compatibilidade_chaves(dict(linha) if isinstance(linha, dict) else {})
    # Regra de negocio solicitada:
    # - Mesmo CHAMADO na mesma DATA => dedup (mantem registro mais completo).
    # - Mesmo CHAMADO em DATA diferente => mantem ambos.
    data_norm = norm(linha.get("DATA", ""))
    chamado_norm = norm(linha.get("CHAMADO", ""))
    if chamado_norm:
        return (data_norm, chamado_norm)

    # Sem chamado nao aplicamos deduplicacao agressiva para evitar perda de linhas.
    return (
        "__SEM_CHAMADO__",
        data_norm,
        norm(linha.get("TÉCNICO", "")),
        norm(linha.get("CLIENTE", "")),
        norm(linha.get("INICIO DA ATIVIDADE", "")),
        norm(linha.get("TÉRMINO DA ATIVIDADE", "")),
        norm(linha.get("ATIVIDADE REALIZADA", "")),
    )


def sort_key_registro(linha):
    tipo = norm(linha.get("_TIPO_REGISTRO", ""))
    prioridade_tipo = 1 if tipo == "retorno_base" else 0
    return (
        norm(linha.get("TÉCNICO", "")),
        data_sort_key(linha.get("DATA", "")),
        hora_sort_key(linha.get("INICIO DA ATIVIDADE", "")),
        prioridade_tipo,
        linha.get("CHAMADO", ""),
    )


def criar_registro_retorno_base(info_ultimo, arq_origem, tecnicos_regras=None):
    tecnico = limpar(info_ultimo.get("TÉCNICO", ""))
    data = limpar(info_ultimo.get("DATA", ""))
    ultimo_endereco_cliente = limpar(info_ultimo.get("ULTIMO_ENDERECO_CLIENTE", ""))
    ultimo_km_final = limpar(info_ultimo.get("ULTIMO_KM_FINAL", ""))
    ultimo_termino = normalizar_hora(info_ultimo.get("ULTIMO_TERMINO", ""))

    if not tecnico or not data:
        return None

    regra = regra_tecnico(tecnico, tecnicos_regras=tecnicos_regras)
    if not regra:
        return None

    estado = limpar(regra.get("estado", ""))
    cidade = limpar(regra.get("cidade", ""))
    endereco_base = limpar(regra.get("endereco_partida", ""))
    horario_ini_exp, horario_fim_exp = expediente_tecnico(tecnico, tecnicos_regras=tecnicos_regras)
    if not endereco_base:
        return None

    linha = {k: "" for k in CAMPOS}
    linha["_ARQUIVO_ORIGEM"] = arq_origem
    linha["_LOGS"] = []
    linha["_TEM_INCONSISTENCIA"] = False
    linha["_TIPO_REGISTRO"] = "RETORNO_BASE"

    linha["DATA"] = data
    linha["CHAMADO"] = ""
    linha["CLIENTE"] = RETORNO_CLIENTE_FIXO
    linha["ESTADO"] = estado
    linha["CIDADE"] = cidade
    linha["TÉCNICO"] = tecnico
    linha["DESCRIÇÃO DO CHAMADO"] = RETORNO_DESCRICAO_FIXA
    linha["ENDEREÇO DE PARTIDA"] = ultimo_endereco_cliente
    linha["ENDEREÇO CLIENTE"] = endereco_base
    linha["ATIVIDADE REALIZADA"] = RETORNO_ATIVIDADE_FIXA
    linha["PATRIMÔNIO"] = "1"
    linha["CATEGORIA"] = ""
    linha["STATUS"] = RETORNO_STATUS_FIXO
    linha["QUEM ACOMPANHOU"] = RETORNO_QUEM_ACOMPANHOU_FIXO
    linha["INICIO DA ATIVIDADE"] = ultimo_termino

    termino_retorno = calcular_termino_retorno(ultimo_termino, horario_fim_exp)
    linha["TÉRMINO DA ATIVIDADE"] = termino_retorno

    km_inicial_int = km_para_int(ultimo_km_final)
    if km_inicial_int is not None:
        linha["KM INICIAL"] = str(km_inicial_int)
        km_rota = consultar_km_rota(ultimo_endereco_cliente, endereco_base)
        if km_rota is not None:
            linha["KM FINAL"] = str(km_inicial_int + int(km_rota))
            registrar_alteracao_linha(
                linha,
                "RETORNO BASE - ROTA",
                "KM FINAL",
                linha["KM INICIAL"],
                linha["KM FINAL"],
            )
        else:
            linha["KM FINAL"] = linha["KM INICIAL"]
            registrar_alteracao_linha(
                linha,
                "RETORNO BASE - ROTA INDISPONIVEL",
                "KM FINAL",
                "",
                linha["KM FINAL"],
            )
    else:
        registrar_alteracao_linha(
            linha,
            "RETORNO BASE - KM INICIAL",
            "KM INICIAL",
            "",
            "NAO DEFINIDO (ULTIMO KM FINAL INVALIDO)",
        )

    if not horario_ini_exp or not horario_fim_exp:
        registrar_alteracao_linha(
            linha,
            "RETORNO BASE - EXPEDIENTE",
            "TÉRMINO DA ATIVIDADE",
            linha["TÉRMINO DA ATIVIDADE"],
            "HORARIO EXPEDIENTE NAO INFORMADO",
        )

    return linha


def ler_linhas(arq):
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(arq, encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue
    with open(arq, encoding="utf-8", errors="ignore") as f:
        return f.readlines()


# -------------------------
# BLOCO J: PROCESSAMENTO DE ARQUIVO TXT
# -------------------------
# Expande chamados, aplica regras sequenciais e filtro por intervalo de datas.
def montar_linhas(arq, data_inicio=None, data_fim=None, tecnicos_regras=None):
    linhas = ler_linhas(arq)
    rats = extrair_rats(linhas)
    resultado = []

    for bloco in rats:
        d = parse_rat(bloco, tecnicos_regras=tecnicos_regras)

        # Regra: RAT sem atividade realizada não entra no processamento.
        if not limpar(d.get("ATIVIDADE REALIZADA", "")):
            continue

        chamados = extrair_chamados(d["CHAMADO"])
        if not chamados:
            chamados = [""]

        for chamado in chamados:
            linha = d.copy()
            linha["CHAMADO"] = chamado
            linha["_ARQUIVO_ORIGEM"] = arq
            linha["_LOGS"] = []
            linha["_TEM_INCONSISTENCIA"] = False
            tecnico_original = linha.get("TÉCNICO", "")
            regra_origem = regra_tecnico(tecnico_original, tecnicos_regras=tecnicos_regras)
            tecnico_mapeado = mapear_tecnico_saida(tecnico_original, tecnicos_regras=tecnicos_regras)
            if limpar(tecnico_mapeado) != limpar(tecnico_original):
                linha["TÉCNICO"] = tecnico_mapeado
                registrar_alteracao_linha(
                    linha,
                    "PADRONIZACAO TECNICO",
                    "TÉCNICO",
                    tecnico_original,
                    tecnico_mapeado,
                )
            if regra_origem and regra_origem.get("origem") == "UI_AVANCADO":
                registrar_alteracao_linha(
                    linha,
                    "TECNICO_UI_AVANCADO",
                    "ORIGEM REGRA TÉCNICO",
                    "CONFIG",
                    "UI_AVANCADO",
                )
            resultado.append(linha)

    resultado.sort(key=sort_key_registro)

    ultimo_endereco = {}
    ultimo_registro_mesmo_dia = {}
    ultimo_info_retorno = {}
    mascara_endereco = ENDERECO_REGRAS.get("mascara_valor", "-")
    for idx_resultado, r in enumerate(resultado):
        chave = (norm(r["TÉCNICO"]), r["DATA"])

        if chave not in ultimo_endereco:
            r["ENDEREÇO DE PARTIDA"] = base_tecnico(r["TÉCNICO"], tecnicos_regras=tecnicos_regras)
        else:
            r["ENDEREÇO DE PARTIDA"] = ultimo_endereco[chave]

        if KM_REGRAS.get("limpar_quando_uber", True) and km_texto_contem_uber(
            r["KM INICIAL"], r["KM FINAL"]
        ):
            km_inicial_ant = r["KM INICIAL"]
            km_final_ant = r["KM FINAL"]
            r["KM INICIAL"] = ""
            r["KM FINAL"] = ""
            registrar_alteracao_linha(
                r,
                "LIMPEZA KM UBER",
                "KM INICIAL",
                km_inicial_ant,
                r["KM INICIAL"],
            )
            registrar_alteracao_linha(
                r,
                "LIMPEZA KM UBER",
                "KM FINAL",
                km_final_ant,
                r["KM FINAL"],
            )
        elif r["KM INICIAL"] and not r["KM FINAL"]:
            km_final_ant = r["KM FINAL"]
            km_ini_int = km_para_int(r["KM INICIAL"])
            incremento = KM_REGRAS.get("incremento_km_final_ausente", 10)
            if km_ini_int is not None and incremento > 0:
                r["KM FINAL"] = str(km_ini_int + incremento)
                registrar_alteracao_linha(
                    r,
                    "KM FINAL AUTOMÁTICO",
                    "KM FINAL",
                    km_final_ant,
                    r["KM FINAL"],
                )

        # snapshot sem máscaras finais para comparar com próximos chamados.
        # inclui o KM FINAL já corrigido com +10 quando aplicável.
        snapshot = {
            "KM INICIAL": r["KM INICIAL"],
            "KM FINAL": r["KM FINAL"],
            "ENDEREÇO DE PARTIDA": r["ENDEREÇO DE PARTIDA"],
            "ENDEREÇO CLIENTE": r["ENDEREÇO CLIENTE"],
        }
        candidato_info = {
            "DATA": r.get("DATA", ""),
            "TÉCNICO": r.get("TÉCNICO", ""),
            "ULTIMO_ENDERECO_CLIENTE": snapshot.get("ENDEREÇO CLIENTE", ""),
            "ULTIMO_KM_FINAL": snapshot.get("KM FINAL", ""),
            "ULTIMO_INICIO": r.get("INICIO DA ATIVIDADE", ""),
            "ULTIMO_TERMINO": r.get("TÉRMINO DA ATIVIDADE", ""),
            "_ORDEM": idx_resultado,
        }
        info_atual = ultimo_info_retorno.get(chave)
        if deve_substituir_info_retorno(info_atual, candidato_info):
            ultimo_info_retorno[chave] = candidato_info

        anterior = ultimo_registro_mesmo_dia.get(chave)
        if anterior:
            if KM_REGRAS.get("mascarar_repetido_mesmo_dia", True) and (
                mesmo_valor(r["KM INICIAL"], anterior["KM INICIAL"])
                and mesmo_valor(r["KM FINAL"], anterior["KM FINAL"])
            ):
                km_inicial_ant = r["KM INICIAL"]
                km_final_ant = r["KM FINAL"]
                r["KM INICIAL"] = ""
                r["KM FINAL"] = ""
                registrar_alteracao_linha(
                    r,
                    "KM REPETIDO",
                    "KM INICIAL",
                    km_inicial_ant,
                    r["KM INICIAL"],
                )
                registrar_alteracao_linha(
                    r,
                    "KM REPETIDO",
                    "KM FINAL",
                    km_final_ant,
                    r["KM FINAL"],
                )

            if ENDERECO_REGRAS.get("mascarar_repetido_mesmo_dia", True) and (
                mesmo_valor(r["ENDEREÇO DE PARTIDA"], anterior["ENDEREÇO DE PARTIDA"])
                and mesmo_valor(r["ENDEREÇO CLIENTE"], anterior["ENDEREÇO CLIENTE"])
            ):
                end_part_ant = r["ENDEREÇO DE PARTIDA"]
                end_cli_ant = r["ENDEREÇO CLIENTE"]
                r["ENDEREÇO DE PARTIDA"] = mascara_endereco
                r["ENDEREÇO CLIENTE"] = mascara_endereco
                registrar_alteracao_linha(
                    r,
                    "ENDEREÇO REPETIDO",
                    "ENDEREÇO DE PARTIDA",
                    end_part_ant,
                    r["ENDEREÇO DE PARTIDA"],
                )
                registrar_alteracao_linha(
                    r,
                    "ENDEREÇO REPETIDO",
                    "ENDEREÇO CLIENTE",
                    end_cli_ant,
                    r["ENDEREÇO CLIENTE"],
                )

        if ENDERECO_REGRAS.get("mascarar_iguais_no_registro", True) and mesmo_valor(
            r["ENDEREÇO DE PARTIDA"], r["ENDEREÇO CLIENTE"]
        ):
            end_part_ant = r["ENDEREÇO DE PARTIDA"]
            end_cli_ant = r["ENDEREÇO CLIENTE"]
            r["ENDEREÇO DE PARTIDA"] = mascara_endereco
            r["ENDEREÇO CLIENTE"] = mascara_endereco
            registrar_alteracao_linha(
                r,
                "ENDEREÇO IGUAL NO REGISTRO",
                "ENDEREÇO DE PARTIDA",
                end_part_ant,
                r["ENDEREÇO DE PARTIDA"],
            )
            registrar_alteracao_linha(
                r,
                "ENDEREÇO IGUAL NO REGISTRO",
                "ENDEREÇO CLIENTE",
                end_cli_ant,
                r["ENDEREÇO CLIENTE"],
            )

        validar_qualidade_registro(r, tecnicos_regras=tecnicos_regras)

        if snapshot["ENDEREÇO CLIENTE"]:
            ultimo_endereco[chave] = snapshot["ENDEREÇO CLIENTE"]

        ultimo_registro_mesmo_dia[chave] = snapshot

    retornos = []
    for chave in sorted(ultimo_info_retorno.keys(), key=lambda x: (x[0], data_sort_key(x[1]))):
        info = ultimo_info_retorno[chave]
        retorno = criar_registro_retorno_base(info, arq_origem=arq, tecnicos_regras=tecnicos_regras)
        if not retorno:
            continue
        validar_qualidade_registro(retorno, tecnicos_regras=tecnicos_regras)
        retornos.append(retorno)

    resultado.extend(retornos)
    resultado.sort(key=sort_key_registro)

    linhas_base = resultado
    if data_inicio is not None or data_fim is not None:
        filtradas = []
        for r in resultado:
            d_obj = data_para_date(r["DATA"])
            if d_obj is None:
                continue
            if data_inicio is not None and d_obj < data_inicio:
                continue
            if data_fim is not None and d_obj > data_fim:
                continue
            filtradas.append(r)
        linhas_base = filtradas

    return [forcar_maiusculas(r) for r in linhas_base]


def linha_para_exportacao(linha):
    linha = aplicar_compatibilidade_chaves(dict(linha) if isinstance(linha, dict) else {})
    out = {}
    for campo_interno in CAMPOS:
        campo_saida = MAPA_CAMPOS_EXPORTACAO.get(campo_interno, campo_interno)
        out[campo_saida] = limpar(linha.get(campo_interno, ""))
    out["KM PERCORRIDO"] = calcular_km_percorrido(
        linha.get("KM INICIAL", ""),
        linha.get("KM FINAL", ""),
    )
    return out


def log_para_exportacao(log):
    log = aplicar_compatibilidade_chaves(dict(log) if isinstance(log, dict) else {})
    out = {}
    for campo_interno in COLUNAS_LOG:
        campo_saida = MAPA_COLUNAS_LOG_EXPORTACAO.get(campo_interno, campo_interno)
        out[campo_saida] = limpar(log.get(campo_interno, ""))
    return out


# -------------------------
# BLOCO K: EXPORTACAO FINAL PARA EXCEL
# -------------------------
# Consolida multiplos arquivos, remove duplicados e grava abas DADOS/LOG.
def gerar_excel(
    arquivos,
    saida,
    data_inicio=None,
    data_fim=None,
    filtro_tecnico="",
    filtro_status="",
    filtro_cidade="",
    somente_inconsistencias=False,
    regras_tecnicos_extra=None,
):
    global _ROTA_TENTATIVAS
    global _SERVICO_ROTA_INDISPONIVEL
    _ROTA_TENTATIVAS = 0
    _SERVICO_ROTA_INDISPONIVEL = False
    _CACHE_GEO.clear()
    _CACHE_ROTA.clear()

    if isinstance(arquivos, str):
        arquivos = [arquivos]

    if isinstance(somente_inconsistencias, str):
        valores_true = set(FILTROS_REGRAS.get("somente_inconsistencias_true_values", []))
        somente_inconsistencias = norm(somente_inconsistencias) in valores_true
    else:
        somente_inconsistencias = bool(somente_inconsistencias)

    tecnicos_regras_ativos = montar_tecnicos_regras_ativas(regras_tecnicos_extra)
    data_inicio_dt = normalizar_data_filtro(data_inicio, "Data inicial")
    data_fim_dt = normalizar_data_filtro(data_fim, "Data final")
    if (
        data_inicio_dt is not None
        and data_fim_dt is not None
        and data_inicio_dt > data_fim_dt
    ):
        raise ValueError("Data inicial não pode ser maior que a data final.")

    todas = []
    for arq in arquivos:
        todas.extend(montar_linhas(arq, data_inicio_dt, data_fim_dt, tecnicos_regras=tecnicos_regras_ativos))

    registros_por_chave = {}
    ordem_chaves = []
    logs_deduplicacao = []

    for r in todas:
        chave = chave_duplicidade(r)
        if chave not in registros_por_chave:
            registros_por_chave[chave] = r
            ordem_chaves.append(chave)
            continue

        atual = registros_por_chave[chave]
        score_atual = pontuacao_registro_para_dedup(atual)
        score_novo = pontuacao_registro_para_dedup(r)
        if score_novo >= score_atual:
            removido = atual
            registros_por_chave[chave] = r
            motivo = "REMOVIDO (MANTIDO REGISTRO MAIS COMPLETO/RECENTE)"
        else:
            removido = r
            motivo = "REMOVIDO (DUPLICADO MENOS COMPLETO)"

        logs_deduplicacao.append(
            {
                "ARQUIVO ORIGEM": removido.get("_ARQUIVO_ORIGEM", ""),
                "DATA": removido.get("DATA", ""),
                "CHAMADO": removido.get("CHAMADO", ""),
                "TÉCNICO": removido.get("TÉCNICO", ""),
                "REGRA": "DEDUPLICACAO",
                "CAMPO": "REGISTRO",
                "VALOR ANTERIOR": "REGISTRO DUPLICADO NA CHAVE DATA+CHAMADO",
                "VALOR FINAL": motivo,
            }
        )

    registros_unicos = [registros_por_chave[ch] for ch in ordem_chaves]

    registros_filtrados = [
        r
        for r in registros_unicos
        if registro_passa_filtros(
            r,
            filtro_tecnico=filtro_tecnico,
            filtro_status=filtro_status,
            filtro_cidade=filtro_cidade,
            somente_inconsistencias=somente_inconsistencias,
        )
    ]

    logs = []
    for r in registros_filtrados:
        logs.extend(log_da_linha(r))

    tecnico_filtro_norm = norm(filtro_tecnico)
    for evento in logs_deduplicacao:
        if tecnico_filtro_norm and tecnico_filtro_norm not in norm(evento.get("TÉCNICO", "")):
            continue
        # Eventos de deduplicacao nao possuem STATUS/CIDADE para filtro semantico.
        if FILTROS_REGRAS.get("ignorar_logs_dedup_com_filtro_status_cidade", True) and (
            limpar(filtro_status) or limpar(filtro_cidade)
        ):
            continue
        logs.append(evento)

    dados_export = [linha_para_exportacao(r) for r in registros_filtrados]
    logs_export = [log_para_exportacao(x) for x in logs]

    df = pd.DataFrame(dados_export, columns=CAMPOS_EXPORTACAO)
    df_log = pd.DataFrame(logs_export, columns=COLUNAS_LOG_EXPORTACAO)
    with pd.ExcelWriter(saida) as writer:
        df.to_excel(writer, index=False, sheet_name="DADOS")
        df_log.to_excel(writer, index=False, sheet_name="LOG")

    qtd_duplicados = len(todas) - len(registros_unicos)
    print(
        f"{len(df)} registros gerados com sucesso! "
        f"({qtd_duplicados} duplicados removidos; {len(registros_filtrados)} apos filtros)"
    )
    return {
        "registros_gerados": len(df),
        "registros_totais_lidos": len(todas),
        "registros_unicos": len(registros_unicos),
        "duplicados_removidos": qtd_duplicados,
        "registros_apos_filtros": len(registros_filtrados),
        "logs_gerados": len(df_log),
        "arquivo_saida": saida,
    }

