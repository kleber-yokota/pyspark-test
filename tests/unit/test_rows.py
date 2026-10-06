"""Testes unitarios da logica pura (sem Spark, sem Docker)."""

import pytest

from pyspark_test.rows import (
    InvalidAmountError,
    format_amount_br,
    is_valid_row,
    parse_amount,
)

# ---------------------------------------------------------------- parse_amount

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10", 10.0),
        ("0", 0.0),
        ("56,90", 56.9),
        ("1.234,56", 1234.56),
        ("10.000.000,00", 10_000_000.0),
        (" R$ 1.234,56 ", 1234.56),
        ("R$100", 100.0),
        ("r$ 100", 100.0),
        ("10000000", 10_000_000.0),
    ],
)
def test_parse_amount_validos(raw: str, expected: float) -> None:
    assert parse_amount(raw) == pytest.approx(expected)


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "R$", "abc", "1,234,56", "12,34,567", "-10", "-1,00", "a,b"],
)
def test_parse_amount_invalidos(raw: str) -> None:
    with pytest.raises((InvalidAmountError, ValueError)):
        parse_amount(raw)


# ----------------------------------------------------------- format_amount_br

@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.0, "0,00"),
        (0.5, "0,50"),
        (10.0, "10,00"),
        (56.9, "56,90"),
        (1234.5, "1.234,50"),
        (1_000_000.0, "1.000.000,00"),
        (10_000_000.0, "10.000.000,00"),
    ],
)
def test_format_amount_br(value: float, expected: str) -> None:
    assert format_amount_br(value) == expected


def test_parse_amount_vazio_lanca_com_mensagem() -> None:
    with pytest.raises(InvalidAmountError, match=r"^valor vazio$"):
        parse_amount("   ")


def test_parse_amount_negativo_lanca_com_mensagem() -> None:
    with pytest.raises(InvalidAmountError, match=r"^valor negativo: .+$"):
        parse_amount("-10")


def test_format_amount_br_negativo_lanca_com_mensagem() -> None:
    with pytest.raises(InvalidAmountError, match=r"^valor negativo: .+$"):
        format_amount_br(-0.01)


# ------------------------------------------------------------- is_valid_row

def test_linha_valida() -> None:
    assert is_valid_row({"id": "1", "amount": "1.234,56", "customer": "ACME"})


@pytest.mark.parametrize(
    "row",
    [
        {"id": "", "amount": "10,00", "customer": "ACME"},
        {"id": "   ", "amount": "10,00", "customer": "ACME"},
        {"id": "1", "amount": "", "customer": "ACME"},
        {"id": "1", "amount": "abc", "customer": "ACME"},
        {"id": "1", "customer": "ACME"},  # sem amount
        {},
    ],
)
def test_linhas_invalidas(row: dict) -> None:
    assert not is_valid_row(row)


def test_is_valid_row_nunca_lanca() -> None:
    # Tipos estranhos nao podem propagar excecao; a linha simplesmente nao passa.
    assert is_valid_row({"id": 123, "amount": None, "customer": None}) is False
