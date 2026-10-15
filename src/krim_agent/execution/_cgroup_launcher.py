"""只有可信宿主启动：先自己入组，再降权exec原bubblewrap，不接触模型payload。"""

import ctypes
import os
import signal
import sys


def main() -> None:
    parent, uid, gid, control = map(int, sys.argv[1:5])
    argv = sys.argv[5:]
    libc = ctypes.CDLL(None, use_errno=True)

    def parent_alive() -> None:
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent:
            raise RuntimeError("parent")

    parent_alive()
    try:
        value = (str(os.getpid()) + "\n").encode("ascii")
        if os.write(control, value) != len(value):
            raise RuntimeError("membership")
    finally:
        os.close(control)
    if os.getuid() == 0:
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    if os.getuid() != uid or os.getgid() != gid or uid == 0 or gid == 0:
        raise RuntimeError("identity")
    # 凭据切换会清除父死亡信号，降权后重新设置并核对，不能留下孤儿启动器。
    parent_alive()
    os.execve(argv[0], argv, {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"})


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        # 不回显可信宿主路径、FD或输入。
        raise SystemExit(1) from None
