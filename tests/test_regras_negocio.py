import textwrap

from extrator import (
    atividade_realizada_semantica_valida,
    base_tecnico,
    categoria,
    converter_data,
    deve_substituir_info_retorno,
    detectar_tecnico,
    inferir_quem_acompanhou,
    montar_linhas,
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
    assert normalizar_campo_km("* A PÉ") == ""
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
    assert len(linhas) == 2

    primeira = linhas[0]
    segunda = linhas[1]

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
        "CIDADE": "SÃO PAULO",
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
        "*TÉCNICO:* ALAN",
        "*DATA:* 10/02/2026",
        "*CLIENTE:* CLIENTE Y",
        "*CHAMADO:* 123456",
        "*INÍCIO DA ATIVIDADE* 11:30",
        "*TÉRMINO DA ATIVIDADE* 12:45",
        "*ATIVIDADE REALIZADA:* TESTE",
        "*STATUS DO CHAMADO:* CONCLUÍDO",
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
    assert not atividade_realizada_semantica_valida("*NÚMERO DE PATRIMÔNIO/SERIAL:* 264671")


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
        "ATIVIDADE REALIZADA: ATIVIDADE REALIZADA: FORMATAÇÃO CONCLUÍDA",
        "STATUS DO CHAMADO: CONCLUÍDO",
    ]
    d = parse_rat(bloco)
    assert d["ATIVIDADE REALIZADA"] == "FORMATAÇÃO CONCLUÍDA"
