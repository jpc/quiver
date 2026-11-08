"""Hatchling build hook -- compiles the bvm executor into the wheel.

bvm (quiver/exec/bvm.c) is a plain C program linked statically against zstd, libsodium,
liburing and BLAKE3 (see quiver/exec/build-deps.sh and the Makefile), so the wheel's only
runtime requirement is glibc. Linux only: elsewhere the wheel is pure Python with no executor.

Environment:
  QUIVER_SKIP_BVM=1       skip the build; pure py3-none-any wheel
  QUIVER_WHEEL_PLAT=TAG   platform tag to claim, e.g. manylinux_2_28_x86_64. release.yml builds
                          inside that manylinux container; the default, linux_<machine>, is the
                          honest tag for a wheel built on an arbitrary host.
  DEPS=<prefix>           passed through to make (static deps prefix; default quiver/exec/deps)

A manylinux claim is checked against the binary's GLIBC_* symbol versions: a bvm that needs a
newer glibc than the tag promises fails the build instead of failing on users' machines.
"""

import os
import platform
import re
import subprocess
import sys

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


def _max_glibc(binary: str):
    """Highest GLIBC_x.y version the binary references, via objdump -T; None if unknown."""
    try:
        out = subprocess.run(["objdump", "-T", binary], capture_output=True, text=True,
                             check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    versions = {tuple(int(x) for x in v.split(".")) for v in re.findall(r"GLIBC_(\d+\.\d+)", out)}
    return max(versions) if versions else None


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        if platform.system() != "Linux" or os.environ.get("QUIVER_SKIP_BVM"):
            return

        exec_dir = os.path.join(self.root, "quiver", "exec")
        binary = os.path.join(exec_dir, "bvm")
        if version == "editable":
            # `pip install -e .`: the package runs from the checkout, so default_bvm() finds
            # whatever ./bvm is there. Don't rebuild it behind the developer's back (it may be
            # the binary running jobs depend on); just say so if it is missing.
            if not os.path.isfile(binary):
                print(f"hatch_build: no {binary}; run  quiver/exec/build-deps.sh && make -C "
                      f"quiver/exec bvm", file=sys.stderr)
            return
        # -B: never ship a stale bvm left over from an earlier/other build. PYTHON: the
        # ipc_gen.h generator runs with the build environment's interpreter (it has numpy).
        env = dict(os.environ, PYTHON=sys.executable)
        subprocess.check_call(["make", "-C", exec_dir, "-B", "bvm"], env=env)
        if not os.path.isfile(binary):
            raise SystemExit(f"hatch_build: make did not produce {binary}")

        build_data.setdefault("force_include", {})[binary] = "quiver/exec/bvm"
        build_data["pure_python"] = False
        build_data["infer_tag"] = True

        machine = platform.machine()
        plat = os.environ.get("QUIVER_WHEEL_PLAT") or f"linux_{machine}"
        m = re.fullmatch(r"manylinux_(\d+)_(\d+)_(\w+)", plat)
        if m:
            if m[3] != machine:
                raise SystemExit(f"hatch_build: QUIVER_WHEEL_PLAT={plat} but this host is {machine}")
            claimed = (int(m[1]), int(m[2]))
            needed = _max_glibc(binary)
            if needed is None:
                print("hatch_build: objdump unavailable; cannot verify the manylinux claim",
                      file=sys.stderr)
            elif needed > claimed:
                raise SystemExit(
                    f"hatch_build: bvm needs GLIBC_{needed[0]}.{needed[1]} but the wheel would "
                    f"claim {plat}. Build inside the quay.io/pypa/{plat} container (release.yml) "
                    f"or unset QUIVER_WHEEL_PLAT for a plain linux_{machine} wheel.")
            else:
                print(f"hatch_build: bvm needs GLIBC_{needed[0]}.{needed[1]} <= {plat}: ok")
        build_data["tag"] = f"py3-none-{plat}"
