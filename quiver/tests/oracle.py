"""Reference python walker: the scan-parity oracle (S2: the only place
the old in-process walker survives)."""
import os
import stat as stat_mod
from pathlib import Path

import polars as pl


def walk(root: str) -> pl.DataFrame:
    rootp = Path(root)
    rows, pending = [], [rootp]
    while pending:
        d = pending.pop()
        for e in sorted(os.scandir(d), key=lambda x: x.name):
            st = os.stat(e.path, follow_symlinks=False)
            rel = str(Path(e.path).relative_to(rootp)).replace(os.sep, "/")
            isdir = stat_mod.S_ISDIR(st.st_mode)
            if isdir:
                pending.append(Path(e.path))
            rows.append((rel, 0 if isdir else st.st_size, st.st_mtime_ns,
                         st.st_mode & 0o7777, st.st_uid, st.st_gid, isdir))
    return pl.DataFrame([list(c) for c in zip(*rows)],
                        schema=["path","size","mtime_ns","mode","uid",
                                "gid","is_dir"]).sort("path")
