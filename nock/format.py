"""quiver.nock.format — archive formats and the layout planner.

RawFormat: headerless payloads + nock index. TarFormat extends it with
ustar/PAX headers generated as Polars expressions (GNU-tar-verified).
Layout assignment is a cum_sum query; sort policy is an expression.
"""

from __future__ import annotations

import numpy as np
import polars as pl

BLOCK = 512
NUL = "\x00"

# ──────────────────────────────────────────────────────────────────────────
# 3. Expression toolkit (used by TarFormat)
# ──────────────────────────────────────────────────────────────────────────

def _byte_sum(s: pl.Series) -> pl.Series:
    """Per-row UTF-8 byte sums via np.add.reduceat — pyarrow-free: join
    the (short, ≤100B) strings once, reduceat over the joined buffer."""
    vals = s.to_list()
    buf = np.frombuffer("".join(vals).encode(), dtype=np.uint8)
    lens = np.array([len(v.encode()) for v in vals], dtype=np.int64)
    offs = np.concatenate([[0], np.cumsum(lens)])
    if len(buf) == 0:
        return pl.Series(np.zeros(len(vals), dtype=np.int64))
    sums = np.add.reduceat(buf, np.minimum(offs[:-1], len(buf) - 1))
    sums = sums.astype(np.int64)
    sums[lens == 0] = 0
    return pl.Series(sums)


def byte_sum(e: pl.Expr) -> pl.Expr:
    return e.map_batches(_byte_sum, return_dtype=pl.Int64)


def octal(e: pl.Expr, width: int) -> pl.Expr:
    """Fixed-width octal field: width-1 ASCII digits + NUL."""
    digits = [(e // (8 ** k) % 8).cast(pl.String)
              for k in range(width - 2, -1, -1)]
    return pl.concat_str(digits + [pl.lit(NUL)])


def octal_digit_sum(e: pl.Expr, width: int) -> pl.Expr:
    """Byte-sum contribution of an octal field (digits are '0'+d)."""
    return (pl.sum_horizontal([(e // (8 ** k) % 8) for k in range(width - 1)])
            + 48 * (width - 1))


def pad_nul(e: pl.Expr, n: pl.Expr) -> pl.Expr:
    """Append n NUL *bytes* (str.pad_end counts chars, useless for UTF-8)."""
    return pl.concat_str([e, pl.lit(NUL).repeat_by(n).list.join("")])


def pax_record_len(body_len: pl.Expr) -> pl.Expr:
    """Self-referential '%d %s=%s\\n' length: n = body + decimal_digits(n),
    solved as a when/then ladder."""
    return pl.coalesce([
        pl.when((body_len + d >= (10 ** (d - 1) if d > 1 else 1))
                & (body_len + d < 10 ** d)).then(body_len + d)
        for d in range(1, 9)
    ])


# ──────────────────────────────────────────────────────────────────────────
# 4. Formats: RawFormat base, TarFormat extension
# ──────────────────────────────────────────────────────────────────────────

class RawFormat:
    """Headerless: payloads back to back (optionally aligned), all metadata
    in the Arrow footer. align=4096 makes payloads O_DIRECT-friendly."""

    name = "raw"
    eof_marker = b""

    def __init__(self, align: int = 1):
        self.align = align

    def with_header_cols(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(header_len=pl.lit(0, pl.Int64))
        # no "header" column → writer emits payloads only


class TarFormat(RawFormat):
    """ustar + PAX-long-name compatibility. Everything is inherited except
    the header columns: header_len budgeting and the header bytes them-
    selves are Polars expressions, so generation fuses into the layout
    plan as one vectorized pass. PAX is emitted only for paths > 100
    bytes; sizes must fit the octal field (< 8 GiB) in this prototype."""

    name = "tar"
    eof_marker = b"\0" * (2 * BLOCK)
    PAX_NAME = "@PaxHeader"  # constant ustar name for the 'x' member

    def __init__(self):
        super().__init__(align=BLOCK)

    # -- one 512B ustar block as an expression ---------------------------
    def _ustar(self, name: pl.Expr, name_bsum: pl.Expr, mode: pl.Expr,
               uid: pl.Expr, gid: pl.Expr, size: pl.Expr, mtime: pl.Expr,
               typeflag: str) -> pl.Expr:
        const = 8 * 32 + ord(typeflag) + sum(b"ustar\x0000")  # spaces+flag+magic
        chk = (name_bsum + octal_digit_sum(mode, 8) + octal_digit_sum(uid, 8)
               + octal_digit_sum(gid, 8) + octal_digit_sum(size, 12)
               + octal_digit_sum(mtime, 12) + const)
        return pl.concat_str([
            pad_nul(name, 100 - name.str.len_bytes()),        # name (100B)
            octal(mode, 8), octal(uid, 8), octal(gid, 8),
            octal(size, 12), octal(mtime, 12),
            pl.concat_str([(chk // (8 ** k) % 8).cast(pl.String)
                           for k in range(5, -1, -1)] + [pl.lit(NUL + " ")]),
            pl.lit(typeflag),
            pl.lit(NUL * 100 + "ustar" + NUL + "00" + NUL * 247),
        ])

    def with_header_cols(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        p, size = pl.col("path"), pl.col("size")
        mode, uid, gid = pl.col("mode"), pl.col("uid"), pl.col("gid")
        mtime = pl.col("mtime_ns") // 1_000_000_000

        fits = p.str.len_bytes() <= 100
        rec_len = pax_record_len(p.str.len_bytes() + 7)   # " path=" + "\n"
        pax_data_len = ((rec_len + BLOCK - 1) // BLOCK) * BLOCK

        lf = lf.with_columns(
            name_field=pl.when(fits).then(p).otherwise(pl.lit(self.PAX_NAME)),
            header_len=pl.when(fits).then(pl.lit(BLOCK, pl.Int64))
                         .otherwise(BLOCK + pax_data_len + BLOCK),
        ).with_columns(name_bsum=byte_sum(pl.col("name_field")))

        member = self._ustar(pl.col("name_field"), pl.col("name_bsum"),
                             mode, uid, gid, size, mtime, "0")
        pax_hdr = self._ustar(pl.lit(self.PAX_NAME),
                              pl.lit(sum(self.PAX_NAME.encode()), pl.Int64),
                              pl.lit(0o644, pl.Int64), uid, gid,
                              rec_len, mtime, "x")
        pax_payload = pad_nul(
            pl.concat_str([rec_len.cast(pl.String), pl.lit(" path="),
                           p, pl.lit("\n")]),
            pax_data_len - rec_len)

        header = (pl.when(fits).then(member)
                    .otherwise(pl.concat_str([pax_hdr, pax_payload, member]))
                    .cast(pl.Binary))
        return lf.with_columns(header=header).drop("name_field", "name_bsum")


# ──────────────────────────────────────────────────────────────────────────
# 5. Planner: layout as a query (shared)
# ──────────────────────────────────────────────────────────────────────────

def plan_layout(lf: pl.LazyFrame, fmt: RawFormat,
                base_offset: int = 0, sort: bool = True) -> pl.DataFrame:
    """sort=False → arrival-order layout: the streaming mode. Offsets
    are a running prefix-sum, so layout is incremental by construction;
    only the *sort policy* is a pipeline breaker, and it's optional."""
    a = fmt.align
    lf = lf.filter(~pl.col("is_dir"))
    if sort:
        lf = lf.sort("path")                               # locality policy
    lf = (lf
            .pipe(fmt.with_header_cols)
            .with_columns(payload_len=((pl.col("size") + a - 1) // a) * a)
            .with_columns(block_len=pl.col("header_len") + pl.col("payload_len"))
            .with_columns(offset=base_offset
                          + pl.col("block_len").cum_sum() - pl.col("block_len"))
            .with_columns(data_offset=pl.col("offset") + pl.col("header_len")))
    plan = lf.collect()
    if isinstance(fmt, TarFormat):
        assert plan["size"].max() is None or plan["size"].max() < 8 ** 11, \
            "PAX size records not implemented in prototype"
    return plan


