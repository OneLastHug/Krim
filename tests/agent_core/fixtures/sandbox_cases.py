"""确定性有限payload/屏障数据；不含Runner或隔离实现。"""

NORMAL = "print(2+3)"
DENIED = {
    "ptrace": 101,
    "process_vm_readv": 310,
    "process_vm_writev": 311,
    "mount": 165,
    "unshare": 272,
    "setns": 308,
    "bpf": 321,
    "perf": 298,
    "io_uring": 425,
    "keyctl": 250,
    "add_key": 248,
    "request_key": 249,
    "reboot": 169,
    "open_by_handle": 304,
    "init_module": 175,
    "finit_module": 313,
    "kexec": 246,
    "socket": 41,
    "connect": 42,
}
CASES = [
    "normal",
    "identity",
    "private_control",
    "rootfs",
    "network",
    "device_create",
    "fifo_export",
    "symlink_export",
    "hardlink_export",
    "secret_export",
    "x32",
    "i386",
    "prctl",
    "clone_ns",
    "clone3",
    *DENIED,
    "cpu",
    "memory",
    "pids",
    "output",
    "wall",
    "fd",
    "tmpfs",
    "workfs",
    "fsize",
]


def payload(case):
    if case == "normal":
        return NORMAL
    if case == "identity":
        return """import os,json
s=open('/proc/self/status').read();assert 'NoNewPrivs:\\t1' in s and 'Seccomp:\\t2' in s and 'CapEff:\\t0000000000000000' in s
assert os.getuid()==0
assert set(os.environ)<={'LANG','PATH','LD_LIBRARY_PATH','LC_CTYPE'}
for p in ['/sys/fs/cgroup','/workspace/Krim-agent-core','/home/dev/.codex','/usr/bin/env']:
 assert not os.path.exists(p),p
print('IDENTITY_OK')"""
    if case == "private_control":
        return """import os,errno
results=[]
for p in ['/proc/1/fd','/proc/1/fd/0','/proc/1/mem','/proc/1/environ','/proc/1/root']:
 try:
  if p.endswith('/fd'):os.listdir(p)
  else:fd=os.open(p,os.O_RDONLY);os.close(fd)
 except OSError as e:results.append((p,e.errno))
 else:results.append((p,0))
print('PROC_RESULTS',results,flush=True)
assert all(n in (1,13) for p,n in results),results
fds=[]
for n in os.listdir('/proc/self/fd'):
 try:os.fstat(int(n));fds.append(int(n))
 except OSError:pass
assert set(fds)=={0,1,2},fds
print('CONTROL_PRIVATE')"""
    if case == "rootfs":
        return """import errno
try:open('/rootfs-write-probe','w')
except OSError as e:assert e.errno==errno.EROFS
else:raise AssertionError('rootfs writable')
print('ROOT_RO')"""
    if case == "network":
        return """import socket,errno
for family in [socket.AF_INET,socket.AF_INET6,socket.AF_UNIX]:
 try:socket.socket(family,socket.SOCK_STREAM)
 except OSError as e:assert e.errno==errno.EPERM
 else:raise AssertionError('socket allowed')
print('NO_SOCKET')"""
    if case in DENIED:
        return f"""import ctypes
libc=ctypes.CDLL(None,use_errno=True)
r=libc.syscall({DENIED[case]},-1,0,0,0,0,0)
assert r==-1 and ctypes.get_errno()==1,(r,ctypes.get_errno())
print('DENIED_{case}')"""
    if case == "x32":
        return """import ctypes
libc=ctypes.CDLL(None,use_errno=True);r=libc.syscall(0x40000001,-1,0,0)
assert r==-1 and ctypes.get_errno()==1
print('X32_DENIED')"""
    if case == "i386":
        return """import ctypes,mmap
m=mmap.mmap(-1,4096,prot=7);m.write(bytes.fromhex('b814000000cd80c3'))
f=ctypes.CFUNCTYPE(ctypes.c_int)(ctypes.addressof(ctypes.c_char.from_buffer(m)))
print('BEFORE_BAD_ARCH',flush=True);f();raise AssertionError('bad arch survived')"""
    if case == "prctl":
        return """import ctypes
libc=ctypes.CDLL(None,use_errno=True);r=libc.prctl(4,1,0,0,0)
assert r==-1 and ctypes.get_errno()==1
r=libc.syscall(317,0,0,0);assert r==-1 and ctypes.get_errno()==1
print('NO_RELAX')"""
    if case == "clone_ns":
        return """import ctypes
libc=ctypes.CDLL(None,use_errno=True);r=libc.syscall(56,0x10000000|17,0,0,0,0)
assert r==-1 and ctypes.get_errno()==1
print('NO_NEWNS')"""
    if case == "clone3":
        return """import ctypes
libc=ctypes.CDLL(None,use_errno=True);r=libc.syscall(435,0,0)
assert r==-1 and ctypes.get_errno()==38
print('CLONE3_ENOSYS')"""
    if case == "device_create":
        return """import os,stat,errno
try:os.mknod('device',stat.S_IFCHR|0o600,os.makedev(1,3))
except OSError as e:assert e.errno==errno.EPERM
else:raise AssertionError('device allowed')
print('DEVICE_DENIED')"""
    if case == "fifo_export":
        return "import os;os.mkfifo('fifo');print('FIFO_CREATED')"
    if case == "symlink_export":
        return "import os;os.symlink('/proc/1/fd/0','link');print('LINK_CREATED')"
    if case == "hardlink_export":
        return "import os;open('a','w').write('x');os.link('a','b');print('HARDLINK_CREATED')"
    if case == "secret_export":
        return "open('.env','w').write('synthetic');print('SECRET_NAME_CREATED')"
    if case == "cpu":
        return "print('ENTER',flush=True)\nwhile True:pass"
    if case == "memory":
        return "import time\nprint('ENTER',flush=True)\ntime.sleep(.15)\na=bytearray(180*1024*1024)"
    if case == "pids":
        return """import os,time
print('ENTER',flush=True)
time.sleep(.15)
try:
 while True:
  p=os.fork()
  if p==0:os.setsid();time.sleep(60);os._exit(0)
except OSError as e:print('FORK_ERR',e.errno,flush=True)
"""
    if case == "output":
        return "import os\nwhile True:os.write(1,b'x'*4096)"
    if case == "wall":
        return "import time\nprint('ENTER',flush=True)\ntime.sleep(60)"
    if case == "fd":
        return """import os,errno
fds=[]
try:
 while True:fds.append(os.open('/dev/null',os.O_RDONLY))
except OSError as e:assert e.errno==errno.EMFILE;print('FD_LIMIT',len(fds))
finally:
 for fd in fds:os.close(fd)
"""
    if case in {"tmpfs", "workfs"}:
        return f"""import errno
root={"/tmp" if case == "tmpfs" else "/work"!r}
try:
 for i in range(32):
  with open(root+'/'+str(i),'wb') as f:f.write(b'x'*1048576)
except OSError as e:assert e.errno==errno.ENOSPC;print('SPACE_LIMIT',e.errno)
else:raise AssertionError('tmpfs did not fill')
"""
    if case == "fsize":
        return """import errno
try:
 with open('/tmp/big','wb') as f:f.write(b'x'*1048577)
except OSError as e:assert e.errno==errno.EFBIG;print('FSIZE_LIMIT')
else:raise AssertionError('fsize not enforced')
"""
    raise ValueError(case)


# 重启夹具只编排同一产品Runner；SIGKILL由创建者的pidfd发出。
if __name__ == "__main__":
    import asyncio
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path.cwd() / "src"))
    from krim_agent.execution.linux import SandboxRunner
    from krim_agent.execution.types import Profile, Request

    async def worker():
        e = Path(sys.argv[1])
        evidence = Path(sys.argv[2])
        phase = sys.argv[3]

        async def hold(current):
            if current == phase:
                (evidence / "worker-ready").write_text(current)
                await asyncio.Event().wait()

        runner = SandboxRunner(
            Profile(
                Path(
                    json.loads((e / "evidence/prep-base/BASE-QUALIFIED.json").read_text())[
                        "rootfs"
                    ]["path"]
                ),
                e / "evidence/prep-base/BASE-QUALIFIED.json",
                Path("/sys/fs/cgroup/system.slice/krim-task.service"),
                e / "materials/d1-os",
                evidence,
            ),
            hook=hold,
        )
        try:
            await runner.run(
                Request(
                    (
                        "-c",
                        "import os,time\nprint('ENTER',flush=True)\nif os.fork()==0:os.setsid();time.sleep(60)\ntime.sleep(60)",
                    )
                )
            )
        finally:
            await runner.close()

    asyncio.run(worker())
