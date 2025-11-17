import re
from typing import Any, Optional

import numpy as np


def clean_isbn13(x: Any) -> Optional[str]:
    """
    Limpia cualquier ISBN13:
      - str con decimales ("9780618510825.0")
      - float (9780618510825.0)
      - int
      - strings con basura (" ISBN: 978-0618510825 ")
    Devuelve:
      - string de 13 dígitos si es posible
      - None si no es válido
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None

    # Convertimos a string
    s = str(x).strip()

    # Si es float en string ("9780618510825.0"), eliminar la parte decimal
    if "." in s:
        # Solo si es decimal .0 exacto
        if s.endswith(".0"):
            s = s[:-2]      # quitar ".0"
        else:
            return None     # no son ISBN válidos si tienen otros decimales

    # Eliminar todo lo que no sea dígito
    digits = re.sub(r"[^0-9]", "", s)

    # ISBN13 siempre debe tener exactamente 13 dígitos
    if len(digits) != 13:
        return None

    return digits


def is_valid_isbn13(x: Any) -> bool:
    digits = clean_isbn13(x)
    if digits is None:
        return False

    # Algoritmo de verificación ISBN13
    total = 0
    for i, d in enumerate(digits[:12]):
        n = int(d)
        total += n if i % 2 == 0 else 3 * n

    check = (10 - (total % 10)) % 10
    return check == int(digits[-1])


def isbn13_valid_or_false(x: Any) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    return is_valid_isbn13(str(x))
