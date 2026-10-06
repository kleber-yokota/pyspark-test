"""Fuzz / testes de propriedade (Hypothesis) sobre a logica pura.

Aqui a gente gera entradas aleatorias e checa invariantes — a forma
"pythonica" de fuzz: nao buscar um input especifico, mas garantir que
nenhuma entrada aleatoria quebra uma propriedade.
"""

import re
from contextlib import suppress

from hypothesis import given
from hypothesis import strategies as st

from pyspark_test.rows import format_amount_br, is_valid_row, parse_amount

amounts = st.floats(
    min_value=0,
    max_value=10_000_000,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
)

BR_AMOUNT_RE = re.compile(r"\d{1,3}(\.\d{3})*,\d{2}")


@given(value=amounts)
def test_roundtrip_parse_format(value: float) -> None:
    """parse(format(x)) devolve x arredondado para 2 casas."""
    value = round(value, 2)
    assert parse_amount(format_amount_br(value)) == round(value, 2)


@given(value=amounts)
def test_format_sempre_em_formato_brasileiro(value: float) -> None:
    """format_amount_br sempre produz o formato N.###,## (2 casas)."""
    assert BR_AMOUNT_RE.fullmatch(format_amount_br(round(value, 2)))


@given(value=amounts)
def test_parse_sempre_nao_negativo(value: float) -> None:
    """Qualquer valor formatado volta nao-negativo pelo parse."""
    assert parse_amount(format_amount_br(round(value, 2))) >= 0


@given(value=amounts, customer=st.text(min_size=1, max_size=8))
def test_linha_formatada_sempre_valida(value: float, customer: str) -> None:
    """Se amount sai do format_amount_br e id nao vazio, a linha e valida."""
    row = {"id": "1", "amount": format_amount_br(round(value, 2)), "customer": customer}
    assert is_valid_row(row)


@given(row=st.dictionaries(st.text(min_size=1, max_size=5), st.text(max_size=40)))
def test_is_valid_row_nunca_lanca(row: dict[str, str]) -> None:
    """is_valid_row deve absorver lixo, nunca propagar excecao."""
    is_valid_row(row)  # nao lanca


@given(garbage=st.text(max_size=20))
def test_parse_de_lixo_ou_lanca_ou_aceita(garbage: str) -> None:
    """parse de texto aleatorio: ou valor >= 0, ou ValueError. Nunca outro."""
    with suppress(ValueError):
        assert parse_amount(garbage) >= 0
