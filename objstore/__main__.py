"""Entry point for ``python3 -m objstore``."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
