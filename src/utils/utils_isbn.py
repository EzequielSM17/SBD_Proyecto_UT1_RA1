import re
from typing import Any

import numpy as np


def is_valid_isbn13(value: str) -> bool:
    if not isinstance(value, str):
        return False
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) != 13:
        return False
    try:
        total = 0
        for i, d in enumerate(digits[:12]):
            n = int(d)
            total += n if i % 2 == 0 else 3 * n
        check = (10 - (total % 10)) % 10
        return check == int(digits[-1])
    except Exception:
        return False


def isbn13_valid_or_false(x: Any) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    return is_valid_isbn13(str(x))
