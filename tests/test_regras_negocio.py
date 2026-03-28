import textwrap

import extrator
from extrator import (
    atividade_realizada_semantica_valida,
    aplicar_validacoes_km_avancadas,
    base_tecnico,
    carregar_catalogo_padroes_rats,
    categoria,
    classificar_bloco_padrao_rat,
    converter_data,
    deve_substituir_info_retorno,
    detectar_tecnico,
    inferir_quem_acompanhou,
    montar_linhas,
    montar_endereco_consulta_rota,
    norm,
    normalizar_hora,
    normalizar_campo_km,
    parse_rat,
    registro_passa_filtros,
    validar_qualidade_registro,
)


def test_converter_data_varios_formatos():
    assert converter_data("02/03/26") == "02/03/2026"
    assert converter_data("2026-03-02") == "02/03/2026"
    assert converter_data("02032026") == "02/03/2026"
    assert converter_data("02.03.2026") == "02/03/2026"


def test_normalizar_campo_km_regras_especiais():
    assert normalizar_campo_km("*") == ""
    assert normalizar_campo_km("* 6216") == "6216"
    assert normalizar_campo_km("* A PÃ‰") == ""
    assert normalizar_campo_km("0.1") == "01"
    assert normalizar_campo_km("A PE") == ""
    assert normalizar_campo_km("O") == ""
    assert normalizar_campo_km("XX") == ""
    assert normalizar_campo_km("XXXXX") == ""


def test_status_e_descricao_do_chamado():
    bloco_pendente = [
        "TECNICO: GLAYDSON",
        "DATA: 02/03/26",
        "CLIENTE: CLIENTE A",
        "CHAMADO: 123456",
        "STATUS DO CHAMADO: PENDENTE",
        "ATIVIDADE REALIZADA: ANALISE",
    ]
    d1 = parse_rat(bloco_pendente)
    assert d1["STATUS"] == "RESOLVIDO"
    assert d1["DESCRIÇÃO DO CHAMADO"] == "AVALIAÇÃO"

    bloco_improdutivo = [
        "TECNICO: GLAYDSON",
        "DATA: 02/03/26",
        "CLIENTE: CLIENTE B",
        "CHAMADO: 654321",
        "STATUS DO CHAMADO: IMPRODUTIVO - CLIENTE FECHADO",
        "ATIVIDADE REALIZADA: VISITA TECNICA",
    ]
    d2 = parse_rat(bloco_improdutivo)
    assert d2["STATUS"] == "IMPRODUTIVO"
    assert d2["DESCRIÇÃO DO CHAMADO"] == "IMPRODUTIVO"


def test_categoria_default_periferico():
    assert categoria("sem termos de equipamento conhecidos") == "PERIFERICO"


def test_tecnico_estado_cidade_e_base():
    estado, cidade = detectar_tecnico("Glaydson Técnico de Fortaleza")
    assert estado == "CE"
    assert cidade == "Fortaleza"
    assert "Aldeota" in base_tecnico("Glaydson")


def test_regras_sequenciais_km_e_endereco(tmp_path):
    conteudo = textwrap.dedent(
        """
        TECNICO: GLAYDSON
        DATA: 01/03/26
        CLIENTE: CLIENTE 1
        CHAMADO: 11111
        KM INICIAL: 100
        KM FINAL:
        HORARIO DE INICIO DA ATIVIDADE: 10:00
        HORARIO DE TERMINO DA ATIVIDADE: 10:30
        ENDERECO: RUA ALFA 100
        ATIVIDADE REALIZADA: TESTE DE IMPRESSORA
        STATUS DO CHAMADO: RESOLVIDO

        TECNICO: GLAYDSON
        DATA: 01/03/26
        CLIENTE: CLIENTE 2
        CHAMADO: 22222
        KM INICIAL: 100
        KM FINAL: 110
        HORARIO DE INICIO DA ATIVIDADE: 11:00
        HORARIO DE TERMINO DA ATIVIDADE: 11:30
        ENDERECO: RUA ALFA 100
        ATIVIDADE REALIZADA: TESTE DE IMPRESSORA
        STATUS DO CHAMADO: RESOLVIDO
        """
    ).strip()
    arq = tmp_path / "rats_teste.txt"
    arq.write_text(conteudo, encoding="utf-8")

    linhas = montar_linhas(str(arq))
    linhas_chamado = [l for l in linhas if l.get("CHAMADO")]
    assert len(linhas_chamado) == 2
    assert any(
        (not l.get("CHAMADO")) and l.get("ATIVIDADE REALIZADA") == "RETORNO"
        for l in linhas
    )

    primeira = linhas_chamado[0]
    segunda = linhas_chamado[1]

    assert primeira["KM FINAL"] == "110"
    assert segunda["KM INICIAL"] == ""
    assert segunda["KM FINAL"] == ""

    assert "ALDEOTA" in primeira["ENDEREÇO DE PARTIDA"]
    assert segunda["ENDEREÇO DE PARTIDA"] == "-"
    assert segunda["ENDEREÇO CLIENTE"] == "-"


def test_validacao_qualidade_e_filtro_inconsistencias():
    linha = {
        "DATA": "31/02/2026",
        "CHAMADO": "ABC",
        "TÉCNICO": "TECNICO DESCONHECIDO",
        "_LOGS": [],
        "_TEM_INCONSISTENCIA": False,
        "STATUS": "RESOLVIDO",
        "CIDADE": "SÃƒO PAULO",
    }
    validar_qualidade_registro(linha)

    assert linha["_TEM_INCONSISTENCIA"] is True
    assert any(ev.get("REGRA") == "VALIDACAO QUALIDADE" for ev in linha["_LOGS"])
    assert registro_passa_filtros(linha, somente_inconsistencias=True)
    assert not registro_passa_filtros(linha, filtro_status="IMPRODUTIVO")


def test_normalizar_hora_formatos_irregulares():
    assert normalizar_hora("16:0") == "16:00"
    assert normalizar_hora("08: 52") == "08:52"
    assert normalizar_hora("9:5") == "09:05"
    assert normalizar_hora("17::40") == "17:40"


def test_valor_em_linha_seguinte_para_quem_acompanhou():
    bloco = [
        "TECNICO: ALAN",
        "DATA: 23/03/2026",
        "CLIENTE: CLIENTE X",
        "CHAMADO: 123456",
        "KM INICIAL: 10",
        "KM FINAL: 20",
        "HORARIO DE INICIO DA ATIVIDADE: 10:00",
        "HORARIO DE TERMINO DA ATIVIDADE: 10:30",
        "ENDERECO: RUA ABC 10",
        "ATIVIDADE REALIZADA: TESTE",
        "STATUS DO CHAMADO: RESOLVIDO",
        "NOME DE QUEM ACOMPANHOU A ATIVIDADE:",
        "VINICIUS",
    ]
    d = parse_rat(bloco)
    assert d["QUEM ACOMPANHOU"] == "VINICIUS"


def test_fallback_quem_acompanhou_ti_local():
    valor = inferir_quem_acompanhou(
        "",
        "AO CHEGAR NO LOCAL O TI LOCAL INFORMOU QUE O CHAMADO JA FOI RESOLVIDO.",
        "",
    )
    assert valor == "TI LOCAL"


def test_fallback_quem_acompanhou_nome_explicito():
    valor = inferir_quem_acompanhou(
        "",
        "Atendimento acompanhado por Joao da Silva durante todos os testes.",
        "",
    )
    assert valor == "Joao da Silva"


def test_parse_termino_com_rotulo_asterisco():
    bloco = [
        "*TÃ‰CNICO:* ALAN",
        "*DATA:* 10/02/2026",
        "*CLIENTE:* CLIENTE Y",
        "*CHAMADO:* 123456",
        "*INÃCIO DA ATIVIDADE* 11:30",
        "*TÃ‰RMINO DA ATIVIDADE* 12:45",
        "*ATIVIDADE REALIZADA:* TESTE",
        "*STATUS DO CHAMADO:* CONCLUÃDO",
    ]
    d = parse_rat(bloco)
    assert d["INICIO DA ATIVIDADE"] == "11:30"
    assert d["TÉRMINO DA ATIVIDADE"] == "12:45"


def test_info_retorno_prioriza_maior_km_final():
    atual = {
        "ULTIMO_KM_FINAL": "120",
        "ULTIMO_INICIO": "09:00",
        "ULTIMO_TERMINO": "10:00",
        "ULTIMO_ENDERECO_CLIENTE": "RUA A, 10",
        "_ORDEM": 10,
    }
    candidato = {
        "ULTIMO_KM_FINAL": "150",
        "ULTIMO_INICIO": "",
        "ULTIMO_TERMINO": "",
        "ULTIMO_ENDERECO_CLIENTE": "RUA B, 20",
        "_ORDEM": 11,
    }
    assert deve_substituir_info_retorno(atual, candidato) is True
    assert deve_substituir_info_retorno(candidato, atual) is False


def test_atividade_semantica_valida():
    assert atividade_realizada_semantica_valida("REALIZADO TESTES E AJUSTES NO EQUIPAMENTO")
    assert not atividade_realizada_semantica_valida("*NÃšMERO DE PATRIMÃ”NIO/SERIAL:* 264671")


def test_parse_nao_confunde_patrimonio_com_atividade():
    bloco = [
        "*NOME COMPLETO DO TECNICO:* GUSTAVO CORTIZO",
        "*DATA DO ATENDIMENTO:* 18/03/26",
        "*CLIENTE:* HOSPITAL ZERBINI",
        "*ENDERECO:* AV BRIGADEIRO LUIS ANTONIO",
        "*CHAMADO:* 522707",
        "*KM INICIAL:* 0",
        "*KM FINAL:* 16",
        "*PREVISAO DE CHEGADA NO CLIENTE:* 17:20",
        "*HORARIO DE INICIO DA ATIVIDADE:*",
        "*HORARIO DE TERMINO DE ATIVIDADE:*",
        "*ATIVIDADE REALIZADA:*",
        "*NUMERO DE PATRIMONIO/SERIAL:* 264671",
        "*STATUS DO CHAMADO:*",
    ]
    d = parse_rat(bloco)
    assert d["ATIVIDADE REALIZADA"] == ""
    assert d["INICIO DA ATIVIDADE"] == "17:20"


def test_problema_identificado_nao_substitui_status_do_chamado():
    bloco = [
        "TECNICO: ALAN GOMES",
        "DATA: 19/03/2026",
        "CLIENTE: CLIENTE X",
        "CHAMADO: 999999",
        "PROBLEMA IDENTIFICADO: SSD COM FALHA",
        "ATIVIDADE REALIZADA: TROCA DE COMPONENTE",
        "STATUS DO CHAMADO: PENDENTE",
    ]
    d = parse_rat(bloco)
    assert d["STATUS"] == "RESOLVIDO"
    assert norm(d["DESCRIÇÃO DO CHAMADO"]) == norm("AVALIAÇÃO")


def test_remove_rotulo_duplicado_no_inicio_da_atividade():
    bloco = [
        "TECNICO: GABRIEL",
        "DATA: 05/03/2026",
        "CLIENTE: USCS",
        "CHAMADO: 529198",
        "ATIVIDADE REALIZADA: ATIVIDADE REALIZADA: FORMATAÃ‡ÃƒO CONCLUÃDA",
        "STATUS DO CHAMADO: CONCLUÃDO",
    ]
    d = parse_rat(bloco)
    assert d["ATIVIDADE REALIZADA"] == "FORMATAÃ‡ÃƒO CONCLUÃDA"

def test_autoajuste_outlier_por_rota(monkeypatch):
    monkeypatch.setattr(extrator, "consultar_km_rota", lambda origem, destino: 20)

    registros = [
        {
            "DATA": "02/03/2026",
            "TÉCNICO": "SP_CR_ROBSON SANTOS_22.194.425",
            "CHAMADO": "111111",
            "KM INICIAL": "1000",
            "KM FINAL": "1010",
            "ENDEREÇO DE PARTIDA": "RUA A, 1",
            "ENDEREÇO CLIENTE": "RUA B, 2",
            "_TIPO_REGISTRO": "",
            "_RETORNO_KM_FINAL_BASE": "1010",
            "_RETORNO_ORDEM_IDX": 0,
            "_LOGS": [],
            "_TEM_INCONSISTENCIA": False,
        },
        {
            "DATA": "02/03/2026",
            "TÉCNICO": "SP_CR_ROBSON SANTOS_22.194.425",
            "CHAMADO": "222222",
            "KM INICIAL": "1010",
            "KM FINAL": "1020",
            "ENDEREÇO DE PARTIDA": "RUA B, 2",
            "ENDEREÇO CLIENTE": "RUA C, 3",
            "_TIPO_REGISTRO": "",
            "_RETORNO_KM_FINAL_BASE": "1020",
            "_RETORNO_ORDEM_IDX": 1,
            "_LOGS": [],
            "_TEM_INCONSISTENCIA": False,
        },
        {
            "DATA": "02/03/2026",
            "TÉCNICO": "SP_CR_ROBSON SANTOS_22.194.425",
            "CHAMADO": "333333",
            "KM INICIAL": "1020",
            "KM FINAL": "1100",
            "ENDEREÇO DE PARTIDA": "RUA C, 3",
            "ENDEREÇO CLIENTE": "RUA D, 4",
            "_TIPO_REGISTRO": "",
            "_RETORNO_KM_FINAL_BASE": "1100",
            "_RETORNO_ORDEM_IDX": 2,
            "_LOGS": [],
            "_TEM_INCONSISTENCIA": False,
        },
    ]

    aplicar_validacoes_km_avancadas(registros)
    outlier = registros[2]
    assert outlier["KM FINAL"] == "1040"
    assert outlier["KM PERCORRIDO"] == "20"
    assert "AUTOAJUSTADO_POR_ROTA_OUTLIER" in outlier.get("MOTIVO VALIDAÇÃO KM", "")


def test_revisao_completa_km_dia_maior_150_autoajusta(monkeypatch):
    monkeypatch.setattr(extrator, "consultar_km_rota", lambda origem, destino: 30)

    registros = [
        {
            "DATA": "03/03/2026",
            "TÉCNICO": "SP_CR_BRENO LUCINDO_58.637.346-9",
            "CHAMADO": "333333",
            "KM INICIAL": "1000",
            "KM FINAL": "1200",
            "ENDEREÇO DE PARTIDA": "RUA D, 4",
            "ENDEREÇO CLIENTE": "RUA E, 5",
            "_TIPO_REGISTRO": "",
            "_RETORNO_KM_FINAL_BASE": "1200",
            "_RETORNO_ORDEM_IDX": 0,
            "_LOGS": [],
            "_TEM_INCONSISTENCIA": False,
        },
        {
            "DATA": "03/03/2026",
            "TÉCNICO": "SP_CR_BRENO LUCINDO_58.637.346-9",
            "CHAMADO": "444444",
            "KM INICIAL": "1200",
            "KM FINAL": "1400",
            "ENDEREÇO DE PARTIDA": "RUA E, 5",
            "ENDEREÇO CLIENTE": "RUA F, 6",
            "_TIPO_REGISTRO": "",
            "_RETORNO_KM_FINAL_BASE": "1400",
            "_RETORNO_ORDEM_IDX": 1,
            "_LOGS": [],
            "_TEM_INCONSISTENCIA": False,
        },
    ]

    aplicar_validacoes_km_avancadas(registros)
    assert registros[0]["KM FINAL"] == "1030"
    assert registros[0]["KM PERCORRIDO"] == "30"
    assert "AUTOAJUSTADO_REVISAO_COMPLETA_150KM" in registros[0].get("MOTIVO VALIDAÇÃO KM", "")
    assert registros[1]["KM FINAL"] == "1230"
    assert registros[1]["KM PERCORRIDO"] == "30"


def test_endereco_com_cidade_sem_uf_complementa_sp():
    meta = montar_endereco_consulta_rota(
        "AV AIRTON SENNA DA SILVA 1421. MAUA",
        cliente="UNIAO QUIMICA SANTO AMARO",
        cidade_tecnico="SÃO PAULO",
        estado_tecnico="SP",
        usar_hint_cliente=True,
    )
    assert "MAUA - SP" in meta["consulta"]
    assert meta["origem_inferencia"] in {"ENDERECO_MUNICIPIO", "ENDERECO_ALIAS", "ENDERECO_COMPLEMENTADO_UF"}


def test_classificar_bloco_padrao_rat_conhecido():
    catalogo = carregar_catalogo_padroes_rats(criar_se_ausente=False)
    bloco = [
        "TÉCNICO: ALAN GOMES",
        "DATA: 19/03/2026",
        "CLIENTE: CLIENTE X",
        "CHAMADO: 533576",
        "KM INICIAL: 63507",
        "KM FINAL: 63521",
        "ATIVIDADE REALIZADA: AJUSTE REALIZADO",
        "STATUS DO CHAMADO: CONCLUÍDO",
    ]
    extraido = extrator.extrair_campos(bloco)
    c = classificar_bloco_padrao_rat(bloco, extraido, catalogo)
    assert c["status"] == "CONHECIDO"
    assert c["score"] >= (catalogo["limiar_similaridade"] * 100.0)


def test_classificar_bloco_padrao_rat_desconhecido():
    catalogo = carregar_catalogo_padroes_rats(criar_se_ausente=False)
    bloco = [
        "PROBLEMA IDENTIFICADO: SSD COM FALHA",
        "OBS: CLIENTE SOLICITOU RETORNO",
        "MODELO DO EQUIPAMENTO: DESKTOP DELL",
    ]
    extraido = extrator.extrair_campos(bloco)
    c = classificar_bloco_padrao_rat(bloco, extraido, catalogo)
    assert c["status"] == "DESCONHECIDO"


def test_decisao_usuario_ignorar_bloco_desconhecido(tmp_path):
    conteudo = textwrap.dedent(
        """
        TECNICO: ALAN GOMES
        DATA: 19/03/2026
        CLIENTE: CLIENTE TESTE
        CHAMADO: 533576
        KM INICIAL: 63507
        KM FINAL: 63521
        INICIO DA ATIVIDADE: 16:50
        TÉRMINO DA ATIVIDADE: 17:15
        ENDEREÇO: AV TESTE 100
        ATIVIDADE REALIZADA: AJUSTE REALIZADO
        STATUS DO CHAMADO: CONCLUÍDO
        """
    ).strip()
    arq = tmp_path / "rat_ignorar.txt"
    arq.write_text(conteudo, encoding="utf-8")

    blocos = extrator.extrair_rats(extrator.ler_linhas(str(arq)))
    h = extrator.hash_bloco_rat(blocos[0])
    chave = extrator.chave_bloco_desconhecido(str(arq), h, 0)
    decisoes_chave = {
        chave: {
            "decisao_usuario": "IGNORAR_BLOCO",
            "campos_ajustados": {},
        }
    }
    linhas = montar_linhas(
        str(arq),
        decisoes_blocos_por_chave=decisoes_chave,
        decisoes_blocos_por_hash={},
    )
    assert linhas == []


def test_decisao_usuario_ajustado_confirmado_aplica_campos(tmp_path):
    conteudo = textwrap.dedent(
        """
        TECNICO: ALAN GOMES
        DATA: 19/03/2026
        CLIENTE: CLIENTE TESTE
        CHAMADO: 533576
        KM INICIAL: 63507
        KM FINAL: 63521
        INICIO DA ATIVIDADE: 16:50
        TÉRMINO DA ATIVIDADE: 17:15
        ENDEREÇO: AV TESTE 100
        ATIVIDADE REALIZADA:
        STATUS DO CHAMADO: CONCLUÍDO
        """
    ).strip()
    arq = tmp_path / "rat_ajuste.txt"
    arq.write_text(conteudo, encoding="utf-8")

    blocos = extrator.extrair_rats(extrator.ler_linhas(str(arq)))
    h = extrator.hash_bloco_rat(blocos[0])
    chave = extrator.chave_bloco_desconhecido(str(arq), h, 0)
    decisoes_chave = {
        chave: {
            "decisao_usuario": "AJUSTADO_CONFIRMADO",
            "campos_ajustados": {
                "ATIVIDADE REALIZADA": "AJUSTE MANUAL CONFIRMADO",
                "CLIENTE": "CLIENTE AJUSTADO",
            },
        }
    }
    linhas = montar_linhas(
        str(arq),
        decisoes_blocos_por_chave=decisoes_chave,
        decisoes_blocos_por_hash={},
    )
    linhas_chamado = [l for l in linhas if l.get("CHAMADO") == "533576"]
    assert linhas_chamado
    assert linhas_chamado[0]["ATIVIDADE REALIZADA"] == "AJUSTE MANUAL CONFIRMADO"
    assert linhas_chamado[0]["CLIENTE"] == "CLIENTE AJUSTADO"

