"""
quiver.remotes.s3 — cloud object storage as another scanner + executor pair.

The architecture generalizes exactly as you'd hope: a *source* is anything
that can produce stat batches (the STAT-shaped DataFrame), a *sink* is
anything that consumes command DataFrames and returns completions. The
planner in the middle never knows which is which:

    scan A (filesystem, C/io_uring)  ┐
                                     ├→ Polars join/delta → cmds → executor B
    scan B (S3 ListObjectsV2)        ┘

Works against AWS S3, Backblaze B2 (S3-compatible API), and GCS
(interoperability/XML API) — all via boto3 with an endpoint_url.

Object stores have no dirs, so cloud plans are two epochs (puts, then
deletes) instead of depth ladders. mtime is preserved in object metadata
(x-amz-meta-mtime-ns) since object mtime is upload time — that's what
makes repeated rsync a no-op.
"""

from __future__ import annotations

import os

import polars as pl

from ..wire import OP_COPY, OP_UNLINK, cmd_df, scan

STAT_COLS = ["path", "size", "mtime_ns", "is_dir"]


def s3_client(endpoint_url: str | None = None, **kw):
    """endpoint_url=None → AWS; 'https://s3.us-west-004.backblazeb2.com'
    → B2; 'https://storage.googleapis.com' → GCS interop."""
    import boto3
    return boto3.client("s3", endpoint_url=endpoint_url, **kw)


def scan_s3(client, bucket: str, prefix: str = "") -> pl.DataFrame:
    """S3 listing → STAT-shaped frame (paths relative to prefix).
    mtime_ns comes from our metadata when present (HEAD is avoided:
    list-only scan uses LastModified, the rsync delta falls back to
    size-only for objects not written by us — see delta logic)."""
    rows = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            rows.append((o["Key"][len(prefix):], o["Size"],
                         int(o["LastModified"].timestamp() * 1e9)))
    if not rows:
        return pl.DataFrame(schema={"path": pl.String, "size": pl.Int64,
                                    "mtime_ns": pl.Int64,
                                    "is_dir": pl.Boolean})
    p, s, m = zip(*rows)
    return pl.DataFrame({"path": p, "size": s, "mtime_ns": m,
                         "is_dir": [False] * len(p)})


class S3Executor:
    """Same contract as PipeExecutor.execute: commands in, completions
    out. OP_COPY = upload path→dst_path key; OP_UNLINK = delete key.
    A boto3-thread-pool or aioboto3 engine drops in behind the same
    interface; a Rust S3 executor would speak the identical Arrow
    protocol over pipes."""

    def __init__(self, client, bucket: str, prefix: str = ""):
        self.c, self.bucket, self.prefix = client, bucket, prefix

    def execute(self, cmds: pl.DataFrame) -> pl.DataFrame:
        res, rsz = [], []
        for r in cmds.sort("dep_group", maintain_order=True).to_dicts():
            try:
                if r["opcode"] == OP_COPY:
                    with open(r["path"], "rb") as f:
                        self.c.put_object(
                            Bucket=self.bucket,
                            Key=self.prefix + r["dst_path"], Body=f,
                            Metadata={"mtime-ns": str(r["header_offset"])})
                elif r["opcode"] == OP_UNLINK:
                    self.c.delete_object(Bucket=self.bucket,
                                         Key=self.prefix + r["path"])
                else:
                    raise ValueError(f"opcode {r['opcode']}")
                res.append(0)
            except Exception:
                res.append(-1)
            rsz.append(r["size"])
        return pl.DataFrame({"user_data": cmds["user_data"],
                             "res": pl.Series(res, dtype=pl.Int32),
                             "read_size": pl.Series(rsz, dtype=pl.Int64)})


def _s3_mtimes(client, bucket, prefix, keys) -> dict[str, int]:
    """Our stored mtimes for the delta predicate (HEAD per key — fine for
    prototypes; production batches this or keeps a manifest object)."""
    out = {}
    for k in keys:
        try:
            h = client.head_object(Bucket=bucket, Key=prefix + k)
            out[k] = int(h["Metadata"].get("mtime-ns", -1))
        except Exception:
            out[k] = -1
    return out


def rsync_to_s3(src_root: str, client, bucket: str, prefix: str = "",
                delete: bool = True, engine: str = "auto",
                threads: int = 8) -> pl.DataFrame:
    """Local tree → object store. Scan with one engine (C/io_uring),
    plan in Polars, execute with another (boto3). Delta: size, then
    stored mtime metadata."""
    src = scan(src_root, engine, threads).filter(~pl.col("is_dir"))
    dst = scan_s3(client, bucket, prefix)
    j = src.join(dst, on="path", how="full", suffix="_d", coalesce=True)
    in_src = pl.col("size").is_not_null()
    in_dst = pl.col("size_d").is_not_null()

    cand = j.filter(in_src & in_dst & (pl.col("size") == pl.col("size_d")))
    stored = _s3_mtimes(client, bucket, prefix, cand["path"].to_list())
    dirty = set(cand.filter(pl.col("mtime_ns") != pl.Series(
        [stored[p] for p in cand["path"]]))["path"]) if len(cand) else set()

    ups = j.filter(in_src & (~in_dst | (pl.col("size") != pl.col("size_d"))
                             | pl.col("path").is_in(dirty)))
    dels = j.filter(in_dst & ~in_src) if delete else j.clear()

    cmds = pl.concat([
        cmd_df(len(ups), opcode=[OP_COPY] * len(ups),
               dep_group=pl.Series([0] * len(ups)),
               path=[os.path.join(src_root, p) for p in ups["path"]],
               dst_path=ups["path"].to_list(), size=ups["size"],
               header_offset=ups["mtime_ns"]),      # → object metadata
        cmd_df(len(dels), opcode=[OP_UNLINK] * len(dels),
               dep_group=pl.Series([1] * len(dels)),
               path=dels["path"].to_list()),
    ]).with_columns(user_data=pl.int_range(len(ups) + len(dels),
                                           dtype=pl.UInt64))
    if len(cmds):
        comp = S3Executor(client, bucket, prefix).execute(cmds)
        bad = comp.filter(pl.col("res") < 0)
        if len(bad):
            raise OSError(f"{len(bad)} cloud ops failed")
    return pl.DataFrame({"op": ["put", "delete"],
                         "count": [len(ups), len(dels)]})


# ── ETag-based sync: content delta from the listing alone ────────────────

PART_SIZE = 8 * 1024 * 1024   # deterministic; recorded per-object in any
                              # manifest so the scheme can evolve safely


def expected_etags(paths: list[str], part_size: int = PART_SIZE,
                   engine: str = "auto") -> pl.DataFrame:
    """Local expected S3 ETags + CRC64NVME via the C executor's CKSUM op.
    Returns {path, etag(hex str incl -N suffix), cksum}."""
    from ..wire import PipeExecutor
    ex = PipeExecutor("-", engine=engine)
    try:
        comp = ex.execute(cmd_df(len(paths), opcode=[6] * len(paths),
                                 path=paths,
                                 pad_align=[part_size] * len(paths)))
    finally:
        ex.close()
    comp = comp.sort("user_data")
    return pl.DataFrame({
        "path": paths,
        "etag": [e.hex() + (f"-{p}" if p else "")
                 for e, p in zip(comp["etag"], comp["parts"])],
        "cksum": comp["cksum"],
    })


def _put(client, bucket, key, local, size, part_size):
    if size <= part_size:
        with open(local, "rb") as f:
            client.put_object(Bucket=bucket, Key=key, Body=f)
        return
    mp = client.create_multipart_upload(Bucket=bucket, Key=key)
    parts = []
    with open(local, "rb") as f:
        pn = 1
        while chunk := f.read(part_size):
            r = client.upload_part(Bucket=bucket, Key=key,
                                   UploadId=mp["UploadId"],
                                   PartNumber=pn, Body=chunk)
            parts.append({"PartNumber": pn, "ETag": r["ETag"]})
            pn += 1
    client.complete_multipart_upload(Bucket=bucket, Key=key,
                                     UploadId=mp["UploadId"],
                                     MultipartUpload={"Parts": parts})


def rsync_to_s3_etag(src_root: str, client, bucket: str, prefix: str = "",
                     delete: bool = True, engine: str = "auto",
                     threads: int = 8,
                     part_size: int = PART_SIZE) -> pl.DataFrame:
    """Content-addressed sync: delta = listed ETag vs locally computed
    expectation. Zero HEADs, zero metadata, no mtime heuristics — and
    hash-equal files are never re-uploaded regardless of timestamps.
    Requires ETag=MD5 semantics (no SSE-KMS/SSE-C)."""
    src = scan(src_root, engine, threads).filter(~pl.col("is_dir"))
    local = expected_etags([os.path.join(src_root, p) for p in src["path"]],
                           part_size, engine).with_columns(path=src["path"])
    dst_rows = []
    for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            dst_rows.append((o["Key"][len(prefix):],
                             o["ETag"].strip('"')))
    dst = pl.DataFrame({"path": [r[0] for r in dst_rows],
                        "etag_d": [r[1] for r in dst_rows]}) if dst_rows \
        else pl.DataFrame(schema={"path": pl.String, "etag_d": pl.String})

    j = local.join(src.select("path", "size"), on="path") \
             .join(dst, on="path", how="full", coalesce=True)
    ups = j.filter(pl.col("etag").is_not_null()
                   & (pl.col("etag_d").is_null()
                      | (pl.col("etag") != pl.col("etag_d"))))
    dels = j.filter(pl.col("etag").is_null()) if delete else j.clear()

    for r in ups.to_dicts():
        _put(client, bucket, prefix + r["path"],
             os.path.join(src_root, r["path"]), r["size"], part_size)
    for r in dels.to_dicts():
        client.delete_object(Bucket=bucket, Key=prefix + r["path"])
    return pl.DataFrame({"op": ["put", "delete"],
                         "count": [len(ups), len(dels)]})
