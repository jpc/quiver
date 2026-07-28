import os, subprocess, sys, pytest
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)


@pytest.fixture(scope="session")
def bvm():
    """Build (if stale) and return the bvm executable path."""
    exec_dir = os.path.join(REPO, "quiver", "exec")
    exe, src = os.path.join(exec_dir, "bvm"), os.path.join(exec_dir, "bvm.c")
    if not os.path.exists(exe) or os.path.getmtime(exe) < os.path.getmtime(src):
        subprocess.check_call(["make", "-C", exec_dir, "bvm"])
    return exe
