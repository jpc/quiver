import os, subprocess, sys, pytest
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)


@pytest.fixture(scope="session")
def bvm():
    """The bvm executable. The test suite is allowed to build it -- it is the one context
    where nothing else is running the binary -- but it goes through the Makefile, which
    links to a temp name and renames, so even here it cannot pull the rug out from under a
    process that happens to be mid-flight."""
    exec_dir = os.path.join(REPO, "quiver", "exec")
    exe, src = os.path.join(exec_dir, "bvm"), os.path.join(exec_dir, "bvm.c")
    if not os.path.exists(exe) or os.path.getmtime(exe) < os.path.getmtime(src):
        subprocess.check_call(["make", "-C", exec_dir, "bvm"])
    return exe
