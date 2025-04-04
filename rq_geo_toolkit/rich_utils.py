import os
from typing import Literal


VERBOSITY_MODE = Literal["silent", "transient", "verbose"]
FORCE_TERMINAL = os.getenv("FORCE_TERMINAL_MODE", "false").lower() == "true"
