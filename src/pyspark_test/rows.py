"""Logica pura de validacao e normalizacao de linhas de entrada.

Esta camada nao usa Spark de proposito: ela e o alvo dos testes unitarios,
fuzz (Hypothesis) e de mutacao (mutmut), que rodam sem Docker e em segundos.
A colagem com Spark fica em :mod:`pyspark_test.pipeline`.
"""

from __future__ import annotations

from collections.abc import Mapping

__all__ = ["InvalidAmountError", "format_amount_br", "is_valid_row", "parse_amount"]


class InvalidAmountError(ValueError):
    """Valor monetario que nao pode ser parseado ou e invalido."""


def parse_amount(raw: str) -> float:
    """Converte um valor monetario no formato brasileiro para ``float``.

    Convencao: ponto e separador de milhar, virgula e separador decimal
    (``"1.234,56" -> 1234.56``, ``"56,90" -> 56.9``). Se nao houver virgula,
    o texto e interpretado como numero simples (``"10" -> 10.0``). O prefixo
    ``R$`` e espacos em volta sao ignorados.

    Raises:
        InvalidAmountError: se o texto estiver vazio ou o valor for negativo.
    """
    text = raw.strip().removeprefix("R$").strip()
    if not text:
        raise InvalidAmountError("valor vazio")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    value = float(text)
    if value < 0:
        raise InvalidAmountError(f"valor negativo: {raw!r}")
    return value


def format_amount_br(value: float) -> str:
    """Formata um ``float`` como valor monetario brasileiro (2 casas).

    ``1234.5 -> "1.234,50"``, ``0 -> "0,00"``. E o inverso de :func:`parse_amount`.

    Raises:
        InvalidAmountError: se ``value`` for negativo.
    """
    if value < 0:
        raise InvalidAmountError(f"valor negativo: {value!r}")
    integer, decimal = f"{value:.2f}".split(".")
    integer = f"{int(integer):,}".replace(",", ".")
    return f"{integer},{decimal}"


def is_valid_row(row: Mapping[str, object]) -> bool:
    """Retorna ``True`` se a linha tem ``id`` nao vazio e ``amount`` parseavel.

    Nao lanca excecao: linhas invalidas simplesmente nao passam na validacao.
    """
    if not str(row.get("id") or "").strip():
        return False
    try:
        parse_amount(str(row.get("amount") or ""))
    except (InvalidAmountError, ValueError, TypeError):
        return False
    return True
