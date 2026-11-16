"""Fixed stdlib-only one-shot secret handoff scripts for execution containers.

The host starts ``/usr/bin/python3 -I -c SECRET_WRITE_CODE`` with the bounded
JSON package on stdin. ``secret_wait_bootstrap`` binds only the non-secret token,
refs and original CWD_EXEC code into the command process.
"""
from __future__ import annotations

import hashlib
import json
import math
import re

from krim_agent.development.contracts import CommandRequest
from krim_agent.development.ports import command_digest

SECRET_DIR_PREFIX = "/tmp/krim-secret-"
SECRET_USED_PREFIX = "/tmp/.krim-secret-used-"
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_REF_PATTERN = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")


def _checked_refs(refs: tuple[str, ...] | list[str]) -> list[str]:
    if (type(refs) not in (tuple, list) or not 1 <= len(refs) <= 8
            or any(type(ref) is not str or _REF_PATTERN.fullmatch(ref) is None for ref in refs)
            or len(set(refs)) != len(refs)):
        raise ValueError("invalid secret references")
    return list(refs)


def secret_token(request: CommandRequest, process_id: str) -> str:
    """Derive the non-secret, command- and process-bound delivery token."""
    if type(process_id) is not str or not process_id:
        raise ValueError("invalid process id")
    binding = json.dumps(
        ["krim.project-secret-bootstrap.v1", command_digest(request), process_id],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(binding).hexdigest()


def secret_wait_bootstrap(
    request: CommandRequest,
    process_id: str,
    cwd_exec: str,
    timeout_seconds: float | None,
) -> str:
    """Build the fixed reader wrapper, bounded by the package expiry."""
    refs = _checked_refs(request.authorization.secret_refs)
    token = secret_token(request, process_id)
    if type(cwd_exec) is not str or not cwd_exec:
        raise ValueError("invalid execution bootstrap")
    if (timeout_seconds is not None
            and (type(timeout_seconds) not in (int, float)
                 or not math.isfinite(timeout_seconds) or timeout_seconds <= 0)):
        raise ValueError("invalid execution timeout")
    refs_json = json.dumps(refs, ensure_ascii=True, separators=(",", ":"))
    return (
        "import os\n"
        "os.environ['KRIM_SECRET_TOKEN'] = " + repr(token) + "\n"
        "os.environ['KRIM_SECRET_REFS'] = " + repr(refs_json) + "\n"
        "CWD_EXEC_CODE = " + repr(cwd_exec) + "\n"
        "COMMAND_TIMEOUT = " + repr(timeout_seconds) + "\n"
        + SECRET_WAIT_CODE
    )


# The writer accepts only a bounded stdin package. A 0600 empty tombstone in
# /tmp survives reader cleanup so the same command/process token cannot replay.
SECRET_WRITE_CODE = r'''import base64,json,math,os,re,stat,sys,time
DIR_PREFIX='/tmp/krim-secret-'
USED_PREFIX='/tmp/.krim-secret-used-'
MAX_PACKAGE=32768
MAX_ITEM=8192
MAX_TOTAL=16384
_TOKEN=re.compile(r'[0-9a-f]{64}\Z')
_REF=re.compile(r'[A-Za-z0-9_.:-]{1,128}\Z')
_created=False
_marker_created=False
_dirfd=None
_markerfd=None
_path=None
_used=None
def _pairs(pairs):
 result={}
 for key,value in pairs:
  if key in result: raise ValueError()
  result[key]=value
 return result
def _parse(raw):
 if not raw or len(raw)>MAX_PACKAGE: raise ValueError()
 obj=json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs)
 if type(obj) is not dict or set(obj)!={'token','refs','values','expires_at'}: raise ValueError()
 token=obj['token']; refs=obj['refs']; values=obj['values']; expiry=obj['expires_at']
 if type(token) is not str or _TOKEN.fullmatch(token) is None: raise ValueError()
 if (type(expiry) not in (int,float) or not math.isfinite(expiry)
     or time.time()>=expiry): raise ValueError()
 if (type(refs) is not list or not 1<=len(refs)<=8
     or any(type(r) is not str or _REF.fullmatch(r) is None for r in refs)
     or len(set(refs))!=len(refs)): raise ValueError()
 if type(values) is not list or len(values)!=len(refs): raise ValueError()
 decoded=[]; total=0
 for value in values:
  if type(value) is not str: raise ValueError()
  item=base64.b64decode(value.encode('ascii'),validate=True)
  if len(item)>MAX_ITEM or base64.b64encode(item).decode('ascii')!=value: raise ValueError()
  total+=len(item)
  if total>MAX_TOTAL: raise ValueError()
  decoded.append(item)
 return token,refs,decoded

def _write(name,data):
 flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW
 fd=os.open(name,flags,0o600,dir_fd=_dirfd)
 try:
  info=os.fstat(fd)
  if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid()
      or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1): raise ValueError()
  view=memoryview(data)
  while view:
   count=os.write(fd,view)
   if count<=0: raise ValueError()
   view=view[count:]
 finally: os.close(fd)

def _cleanup():
 global _dirfd,_markerfd
 if _dirfd is not None:
  try:
   for name in os.listdir(_dirfd):
    try: os.unlink(name,dir_fd=_dirfd)
    except IsADirectoryError: os.rmdir(name,dir_fd=_dirfd)
  except BaseException: pass
  try: os.close(_dirfd)
  except BaseException: pass
  _dirfd=None
 if _created and _path is not None:
  try: os.rmdir(_path)
  except BaseException: pass
 if _markerfd is not None:
  try: os.close(_markerfd)
  except BaseException: pass
  _markerfd=None
 if _marker_created and _used is not None:
  try: os.unlink(_used)
  except BaseException: pass

def _main():
 global _created,_marker_created,_dirfd,_markerfd,_path,_used
 raw=sys.stdin.buffer.read(MAX_PACKAGE+1)
 token,refs,values=_parse(raw)
 _path=DIR_PREFIX+token
 _used=USED_PREFIX+token
 os.mkdir(_path,0o700)
 _created=True
 _dirfd=os.open(_path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 info=os.fstat(_dirfd)
 if (not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid()
     or stat.S_IMODE(info.st_mode)!=0o700): raise ValueError()
 marker_flags=os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW
 _markerfd=os.open(_used,marker_flags,0o600)
 _marker_created=True
 marker_info=os.fstat(_markerfd)
 if (not stat.S_ISREG(marker_info.st_mode) or marker_info.st_uid!=os.getuid()
     or stat.S_IMODE(marker_info.st_mode)!=0o600 or marker_info.st_nlink!=1): raise ValueError()
 _write('package',raw)
 for index,value in enumerate(values): _write('secret-'+str(index),value)
 _write('.ready-pending',b'')
 os.rename('.ready-pending','ready',src_dir_fd=_dirfd,dst_dir_fd=_dirfd)
 os.close(_dirfd); _dirfd=None
 os.close(_markerfd); _markerfd=None
try: _main()
except BaseException:
 _cleanup()
 raise SystemExit(78)
'''


# Reader code validates identity/modes/binding and expiry, opens read-only FDs,
# deletes package and private directory, then execs a timeout for original CWD_EXEC.
SECRET_WAIT_CODE = r'''import base64,json,math,os,re,stat,sys,time
DIR_PREFIX='/tmp/krim-secret-'
MAX_PACKAGE=32768
MAX_ITEM=8192
MAX_TOTAL=16384
WAIT_SECONDS=15.0
_TOKEN=re.compile(r'[0-9a-f]{64}\Z')
_REF=re.compile(r'[A-Za-z0-9_.:-]{1,128}\Z')
_dirfd=None
_packagefd=None
_readyfd=None
_secretfds=[]
_path=None

def _pairs(pairs):
 result={}
 for key,value in pairs:
  if key in result: raise ValueError()
  result[key]=value
 return result

def _cleanup():
 global _dirfd,_packagefd,_readyfd,_secretfds
 for fd in (_packagefd,_readyfd):
  if fd is not None:
   try: os.close(fd)
   except BaseException: pass
 _packagefd=None; _readyfd=None
 for fd in _secretfds:
  try: os.close(fd)
  except BaseException: pass
 _secretfds=[]
 if _dirfd is not None:
  try:
   for name in os.listdir(_dirfd):
    try: os.unlink(name,dir_fd=_dirfd)
    except IsADirectoryError: os.rmdir(name,dir_fd=_dirfd)
  except BaseException: pass
  try: os.close(_dirfd)
  except BaseException: pass
  _dirfd=None
 if _path is not None:
  try: os.rmdir(_path)
  except BaseException: pass

def _read(fd,limit):
 chunks=[]; size=0
 while True:
  part=os.read(fd,min(4096,limit+1-size))
  if not part: break
  chunks.append(part); size+=len(part)
  if size>limit: raise ValueError()
 return b''.join(chunks)

def _open_checked(name,mode):
 before=os.stat(name,dir_fd=_dirfd,follow_symlinks=False)
 if (not stat.S_ISREG(before.st_mode) or before.st_uid!=os.getuid()
     or stat.S_IMODE(before.st_mode)!=mode or before.st_nlink!=1): raise ValueError()
 fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=_dirfd)
 after=os.fstat(fd)
 if ((before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino)
     or not stat.S_ISREG(after.st_mode) or after.st_uid!=os.getuid()
     or stat.S_IMODE(after.st_mode)!=mode or after.st_nlink!=1):
  os.close(fd); raise ValueError()
 return fd,after

def _main():
 global _dirfd,_packagefd,_readyfd,_secretfds,_path
 token=os.environ.pop('KRIM_SECRET_TOKEN')
 refs_raw=os.environ.pop('KRIM_SECRET_REFS')
 if type(token) is not str or _TOKEN.fullmatch(token) is None: raise ValueError()
 expected_refs=json.loads(refs_raw)
 if (type(expected_refs) is not list or not 1<=len(expected_refs)<=8
     or any(type(r) is not str or _REF.fullmatch(r) is None for r in expected_refs)
     or len(set(expected_refs))!=len(expected_refs)): raise ValueError()
 expected_json=json.dumps(expected_refs,ensure_ascii=True,separators=(',',':'))
 _path=DIR_PREFIX+token
 deadline=time.monotonic()+WAIT_SECONDS
 while True:
  try: before=os.lstat(_path)
  except FileNotFoundError:
   if time.monotonic()>=deadline: raise ValueError()
   time.sleep(min(.05,max(0.0,deadline-time.monotonic()))); continue
  if (not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode)
      or before.st_uid!=os.getuid() or stat.S_IMODE(before.st_mode)!=0o700): raise ValueError()
  _dirfd=os.open(_path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
  after=os.fstat(_dirfd)
  if ((before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino)
      or not stat.S_ISDIR(after.st_mode) or after.st_uid!=os.getuid()
      or stat.S_IMODE(after.st_mode)!=0o700): raise ValueError()
  break
 while True:
  try: ready_info=os.stat('ready',dir_fd=_dirfd,follow_symlinks=False)
  except FileNotFoundError:
   if time.monotonic()>=deadline: raise ValueError()
   time.sleep(min(.05,max(0.0,deadline-time.monotonic()))); continue
  if (not stat.S_ISREG(ready_info.st_mode) or ready_info.st_uid!=os.getuid()
      or stat.S_IMODE(ready_info.st_mode)!=0o600 or ready_info.st_size!=0
      or ready_info.st_nlink!=1): raise ValueError()
  _readyfd,ready_opened=_open_checked('ready',0o600)
  if ready_opened.st_size!=0: raise ValueError()
  break
 _packagefd,package_info=_open_checked('package',0o600)
 if package_info.st_size>MAX_PACKAGE: raise ValueError()
 raw=_read(_packagefd,MAX_PACKAGE)
 os.close(_packagefd); _packagefd=None
 obj=json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs)
 if type(obj) is not dict or set(obj)!={'token','refs','values','expires_at'}: raise ValueError()
 packet_token=obj['token']; refs=obj['refs']; encoded=obj['values']; expiry=obj['expires_at']
 if packet_token!=token or refs!=expected_refs: raise ValueError()
 if (type(expiry) not in (int,float) or not math.isfinite(expiry)
     or time.time()>=expiry): raise ValueError()
 if (type(refs) is not list or not 1<=len(refs)<=8
     or any(type(r) is not str or _REF.fullmatch(r) is None for r in refs)
     or len(set(refs))!=len(refs) or type(encoded) is not list or len(encoded)!=len(refs)):
  raise ValueError()
 values=[]; total=0
 for item in encoded:
  if type(item) is not str: raise ValueError()
  value=base64.b64decode(item.encode('ascii'),validate=True)
  if len(value)>MAX_ITEM or base64.b64encode(value).decode('ascii')!=item: raise ValueError()
  total+=len(value)
  if total>MAX_TOTAL: raise ValueError()
  values.append(value)
 expected_names={'package','ready'}|{'secret-'+str(i) for i in range(len(values))}
 if set(os.listdir(_dirfd))!=expected_names: raise ValueError()
 for index,value in enumerate(values):
  fd,info=_open_checked('secret-'+str(index),0o600)
  _secretfds.append(fd)
  if info.st_size!=len(value) or _read(fd,MAX_ITEM)!=value: raise ValueError()
  os.lseek(fd,0,os.SEEK_SET)
  os.set_inheritable(fd,True)
 os.close(_readyfd); _readyfd=None
 for name in os.listdir(_dirfd): os.unlink(name,dir_fd=_dirfd)
 os.close(_dirfd); _dirfd=None
 os.rmdir(_path); _path=None
 for key in list(os.environ):
  if key.startswith('KRIM_SECRET_FD_'): del os.environ[key]
 for index,fd in enumerate(_secretfds): os.environ['KRIM_SECRET_FD_'+str(index)]=str(fd)
 os.environ['KRIM_SECRET_REFS']=expected_json
 del raw,obj,encoded,values,expected_refs,refs_raw
 remaining=expiry-time.time()
 if remaining<=0: raise ValueError()
 effective=remaining if COMMAND_TIMEOUT is None else min(remaining,COMMAND_TIMEOUT)
 nanos=int(effective*1000000000)
 if nanos<=0: raise ValueError()
 seconds,nanos=divmod(nanos,1000000000)
 duration=str(seconds)+'.'+format(nanos,'09d')+'s'
 argv=['/usr/bin/timeout','--foreground','--signal=KILL',duration,
       '/usr/bin/python3','-I','-c',CWD_EXEC_CODE,sys.argv[1],sys.argv[2]]
 os.execv('/usr/bin/timeout',argv)
 raise ValueError()
try: _main()
except BaseException:
 _cleanup()
 raise SystemExit(78)
'''
