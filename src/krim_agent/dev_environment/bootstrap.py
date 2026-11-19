"""Immutable argument bootstrap for the UID65532 execution container's PID 1."""

# The command runs as the container's PID 1. Exiting PID 1 tears down detached
# descendants as well; C additionally requests B.cancel on timeout/revocation.
# Python -I is the image's system interpreter, independent of project imports.
CWD_EXEC = """import os,sys,stat
parts=sys.argv[1].split('/')
fd=os.open('/workspace',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:
 for part in parts:
  if part in ('','.'): continue
  if part=='..': raise ValueError('invalid cwd')
  before=os.stat(part,dir_fd=fd,follow_symlinks=False)
  if not stat.S_ISDIR(before.st_mode): raise ValueError('cwd is not a directory')
  child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
  after=os.fstat(child)
  if (before.st_dev,before.st_ino)!=(after.st_dev,after.st_ino):
   os.close(child); raise ValueError('cwd changed')
  os.close(fd); fd=child
 os.fchdir(fd)
finally: os.close(fd)
os.execv('/bin/bash',['/bin/bash','--noprofile','--norc','-c',sys.argv[2]])
"""
