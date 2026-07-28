"""Deprecated alias — the BLOCKS engine is now the main `quiver` CLI (quiver/cli.py).
`python -m quiver.exec.blocks_cli ...` forwards to it so old invocations keep working."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from quiver.cli import main

if __name__ == "__main__":
    main()
