"""Fixed stdlib-only loopback proxy shipped over Docker exec, never imported there.

The sandbox is untrusted. Only the host's parsed download messages can open an
external socket; neither CONNECT nor TLS bytes pass through to the network.
"""

PROXY_PORT = 39119
CA_PATH = "/home/krim/.krim-network/ca.pem"
READY_PATH = "/home/krim/.krim-network/ready"

WAIT_CODE = r'''import os,time
for _ in range(300):
 if os.path.isfile('/home/krim/.krim-network/ready'): break
 time.sleep(.05)
else: raise SystemExit(78)
'''

SHIM_CODE = r'''import asyncio,base64,json,os,ssl,sys,time
from urllib.parse import urlsplit
PORT=39119
ROOT='/home/krim/.krim-network'
MAX=131072
async def main():
 loop=asyncio.get_running_loop()
 reader=asyncio.StreamReader(limit=MAX)
 await loop.connect_read_pipe(lambda:asyncio.StreamReaderProtocol(reader),sys.stdin.buffer)
 transport,protocol=await loop.connect_write_pipe(
  asyncio.streams.FlowControlMixin,sys.stdout.buffer)
 output=asyncio.StreamWriter(transport,protocol,None,loop)
 line=await asyncio.wait_for(reader.readline(),10)
 if len(line)>MAX: raise ValueError()
 config=json.loads(line)
 age=float(config['age'])
 if not 1<=age<=900: raise ValueError()
 os.mkdir(ROOT,0o700)
 for filename,value in [('ca.pem',config['cert']),('key.pem',config['key'])]:
  fd=os.open(ROOT+'/'+filename,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
  with os.fdopen(fd,'w') as f: f.write(value)
 context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
 context.minimum_version=ssl.TLSVersion.TLSv1_2
 context.set_alpn_protocols(['http/1.1'])
 context.load_cert_chain(ROOT+'/ca.pem',ROOT+'/key.pem')
 os.unlink(ROOT+'/key.pem')
 lock=asyncio.Lock(); pending={}; connections=set(); serial=0; last=time.monotonic()
 async def send(message):
  async with lock:
   output.write(json.dumps(message,separators=(',',':')).encode()+b'\n')
   await asyncio.wait_for(output.drain(),5)
 async def dispatch():
  nonlocal last
  while True:
   line=await reader.readline()
   if not line or len(line)>MAX: raise EOFError()
   m=json.loads(line); last=time.monotonic()
   if m=={'ping':True}: continue
   rid=m.get('id')
   if rid not in pending: raise ValueError()
   await asyncio.wait_for(pending[rid].put(m),10)
 async def header(r):
  raw=await asyncio.wait_for(r.readuntil(b'\r\n\r\n'),10)
  if len(raw)>32768: raise ValueError()
  rows=raw[:-4].decode('ascii').split('\r\n')
  parts=rows.pop(0).split(' ')
  if len(parts)!=3 or parts[2]!='HTTP/1.1': raise ValueError()
  headers={}
  for row in rows:
   k,sep,v=row.partition(':')
   if not sep or not k or any(not(c.isalnum() or c in "!#$%&'*+-.^_`|~") for c in k):
    raise ValueError()
   k=k.lower(); v=v.strip(' ')
   if k in headers or any(ord(c)<32 or ord(c)==127 for c in v): raise ValueError()
   headers[k]=v
  if len(headers)>64 or 'transfer-encoding' in headers or 'upgrade' in headers: raise ValueError()
  if headers.get('content-length','0')!='0': raise ValueError()
  return parts[0],parts[1],headers
 async def client(r,w):
  nonlocal serial
  task=asyncio.current_task(); connections.add(task); rid=None; started=False
  try:
   if len(connections)>8: raise ValueError()
   method,authority,headers=await header(r)
   if method!='CONNECT': raise ValueError()
   url=urlsplit('https://'+authority)
   if (not url.hostname or url.username is not None or url.password is not None
       or url.path or url.query or url.fragment or '@' in authority
       or not authority.isascii() or ':' not in authority): raise ValueError()
   port=url.port
   if port is None: raise ValueError()
   w.write(b'HTTP/1.1 200 Connection Established\r\n\r\n'); await w.drain()
   await w.start_tls(context,ssl_handshake_timeout=10)
   method,path,headers=await header(r)
   expected=url.hostname.lower().rstrip('.')
   host=urlsplit('https://'+headers.get('host',''))
   if (method not in ('GET','HEAD') or not path.startswith('/') or path.startswith('//')
       or len(path)>16384 or '#' in path or chr(92) in path or host.username is not None
       or host.password is not None or host.path or host.query or host.fragment
       or (host.hostname or '').lower().rstrip('.')!=expected
       or (host.port or 443)!=port): raise ValueError()
   serial+=1; rid=serial; queue=asyncio.Queue(maxsize=2); pending[rid]=queue
   await send({'id':rid,'method':method,'url':'https://'+authority+path,'headers':headers})
   response=await asyncio.wait_for(queue.get(),40)
   if 'status' not in response: raise ValueError()
   status=response['status']
   if type(status) is not int or not 100<=status<=599: raise ValueError()
   data=('HTTP/1.1 '+str(status)+' Response\r\n').encode()
   for k,v in response['headers'].items():
    if any(ord(c)<32 or ord(c)==127 for c in k+v): raise ValueError()
    data+=(k+': '+v+'\r\n').encode('latin1')
   w.write(data+b'Connection: close\r\n\r\n'); await w.drain(); started=True
   while True:
    m=await asyncio.wait_for(queue.get(),40)
    if m.get('end') is True: break
    if 'error' in m: raise ValueError()
    chunk=base64.b64decode(m['data'],validate=True)
    if not 0<len(chunk)<=32768: raise ValueError()
    w.write(chunk); await asyncio.wait_for(w.drain(),10)
  except Exception:
   if not started:
    try:
     w.write(b'HTTP/1.1 403 Download denied\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
     await asyncio.wait_for(w.drain(),1)
    except Exception: pass
  finally:
   if rid is not None: pending.pop(rid,None)
   connections.discard(task); w.close()
   try: await asyncio.wait_for(w.wait_closed(),1)
   except Exception: pass
 server=await asyncio.start_server(client,'127.0.0.1',PORT,limit=32768)
 with open(ROOT+'/ready','x') as f: f.write('ready')
 await send({'ready':True})
 async def health():
  born=time.monotonic()
  while time.monotonic()-born<age and time.monotonic()-last<5:
   await asyncio.sleep(.25)
 tasks=[asyncio.create_task(dispatch()),asyncio.create_task(health())]
 try:
  await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
 finally:
  server.close(); await server.wait_closed()
  for t in tasks+list(connections): t.cancel()
  await asyncio.gather(*tasks,*list(connections),return_exceptions=True)
  try: os.unlink(ROOT+'/ready')
  except OSError: pass
  output.close()
asyncio.run(main())
'''
