from typing import List
from src.config import config

async def get_available_symbols() -> List[str]:
    """
    Returns the list of all symbols that the pipeline should process.
    Retrieves data from the configuration file (config.py).

    Keeps 'async' and 'session' for compatibility with other files
    that call this function via 'await'.
    """

    # 1. Read list directly from config
    symbol_list = config.get("symbols", [])

    # 2. Check: if list is empty, return default or empty list
    if not symbol_list:
        print("⚠️ [SYMBOLS] No processing symbols found in config.py!")
        return []

    return symbol_list
