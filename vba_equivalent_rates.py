from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Sequence

from dateutil.relativedelta import relativedelta


def _to_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _one_year_back_same_day(dt: date) -> date:
    return dt + relativedelta(years=-1)


def _days_in_previous_year_window(dt: date) -> int:
    return (dt - _one_year_back_same_day(dt)).days


def mati(date_debut: date | datetime, i: int) -> int:
    d0 = _to_date(date_debut)
    d1 = d0 + relativedelta(months=12 * i)
    return (d1 - d0).days


def _linear_interp(x: float, x0: float, y0: float, x1: float, y1: float) -> float:
    if x1 == x0:
        return y0
    return ((x - x0) * (y1 - y0) / (x1 - x0)) + y0


def _validate_curve(mt: Sequence[float], tx: Sequence[float]) -> None:
    if len(mt) != len(tx):
        raise ValueError("mt and tx must have same length")
    if len(mt) < 2:
        raise ValueError("curve must contain at least 2 points")


def calcul_taux(
    maturity: float,
    mt: Sequence[float],
    tx: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _validate_curve(mt, tx)
    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    last = len(mt) - 1

    if maturity <= mt[0]:
        return tx[0]

    if mt[0] < maturity <= mt[last]:
        for i in range(last):
            b = mt[i]
            a = mt[i + 1]
            if b <= maturity <= a:
                if a <= seuil_1an or b > seuil_1an:
                    return _linear_interp(maturity, b, tx[i], a, tx[i + 1])

                if a > seuil_1an and b <= seuil_1an:
                    if maturity > seuil_1an:
                        date_b = d0 + timedelta(days=int(b))
                        base = _days_in_previous_year_window(date_b)
                        taux = ((1 + tx[i] * b / 360) ** (base / b)) - 1
                        return _linear_interp(maturity, b, taux, a, tx[i + 1])

                    date_a = d0 + timedelta(days=int(a))
                    base = _days_in_previous_year_window(date_a)
                    taux = (360 / a) * (((1 + tx[i + 1]) ** (a / base)) - 1)
                    return _linear_interp(maturity, b, tx[i], a, taux)

        return tx[last]

    return _linear_interp(
        maturity,
        mt[last - 1],
        tx[last - 1],
        mt[last],
        tx[last],
    )


def interpol(
    arg: bool,
    maturity: float,
    mtz: Sequence[float],
    txz: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _validate_curve(mtz, txz)
    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    last = len(mtz) - 1

    if arg:
        if maturity <= mtz[0]:
            return txz[0]
        if maturity >= mtz[last]:
            return _linear_interp(
                maturity,
                mtz[last - 1],
                txz[last - 1],
                mtz[last],
                txz[last],
            )
        for i in range(last):
            b = mtz[i]
            a = mtz[i + 1]
            if b <= maturity <= a:
                return _linear_interp(maturity, b, txz[i], a, txz[i + 1])
        return txz[last]

    if maturity <= mtz[0]:
        p = txz[0]
    elif maturity >= mtz[last]:
        p = _linear_interp(
            maturity,
            mtz[last - 1],
            txz[last - 1],
            mtz[last],
            txz[last],
        )
    else:
        p = txz[0]
        for i in range(last):
            b = mtz[i]
            a = mtz[i + 1]
            if not (b <= maturity <= a):
                continue
            if a <= seuil_1an or b > seuil_1an:
                p = _linear_interp(maturity, b, txz[i], a, txz[i + 1])
            else:
                date_b = d0 + timedelta(days=int(b))
                base_b = _days_in_previous_year_window(date_b)
                taux_b_actu = ((1 + txz[i] * b / 360) ** (base_b / b)) - 1
                p_actu = _linear_interp(maturity, b, taux_b_actu, a, txz[i + 1])
                if maturity < seuil_1an:
                    alpha = _days_in_previous_year_window(d0 + timedelta(days=int(maturity)))
                    p = (360 / maturity) * (((1 + p_actu) ** (maturity / alpha)) - 1)
                else:
                    p = p_actu
            break
    return p


def conversion_actu_monnaitaire(
    arg: bool,
    maturity: float,
    date_flux: date | datetime | None,
    mt: Sequence[float],
    tx: Sequence[float],
    date_debut: date | datetime,
) -> float:
    _ = date_flux
    if maturity == 0:
        return 0.0

    d0 = _to_date(date_debut)
    seuil_1an = mati(d0, 1)
    taux = calcul_taux(maturity, mt, tx, d0)

    if arg is False:
        if maturity <= seuil_1an:
            alpha_date = d0 + timedelta(days=int(maturity))
            alpha = _days_in_previous_year_window(alpha_date)
            return ((1 + taux * maturity / 360) ** (alpha / maturity)) - 1
        return taux

    if maturity >= seuil_1an:
        return (360 / maturity) * (((1 + taux) ** (maturity / 365)) - 1)
    return taux

