"""Fixed stdlib-only relay source executed inside the development container.

The module intentionally exposes only ``RELAY_CODE``.  The embedded program
has no import-time dependency on Krim or on a host-side protocol module.
"""

RELAY_CODE = r'''import errno
import math
import os
import selectors
import socket
import struct
import sys
import time
from collections import deque

_HEADER = struct.Struct("!BI")
_COUNT = struct.Struct("!I")
_MAX_PAYLOAD = 65536
_WINDOW = 262144
_CONTROL_LIMIT = 64
_CONTROL_BYTES = 4096
_OUTPUT_LIMIT = _WINDOW + _CONTROL_BYTES
_SHUTDOWN_SECONDS = 1.0

_DATA = 1
_FIN = 2
_PING = 3
_CLOSE = 4
_CREDIT = 5

_READY = 10
_REMOTE_DATA = 11
_EOF = 12
_CONNECT_FAILED = 13
_REMOTE_CREDIT = 14
_DONE = 15

# The process never includes exception text, paths, argv or the token in stderr.
_ERR_INVALID_ARGUMENTS = b"relay invalid arguments\n"
_ERR_PROTOCOL = b"relay protocol failure\n"
_ERR_RUNTIME = b"relay runtime failure\n"


def _stderr(message):
    try:
        os.write(2, message)
    except BaseException:
        pass


def _number(value, maximum):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError from None
    if not math.isfinite(result) or result <= 0.0 or result > maximum:
        raise ValueError
    return result


def _arguments(argv):
    if len(argv) != 6:
        raise ValueError
    port_text, connect_text, heartbeat_text, age_text, idle_text, token = argv
    if not port_text.isascii() or not port_text.isdecimal():
        raise ValueError
    port = int(port_text, 10)
    if not 1 <= port <= 65535:
        raise ValueError
    connect_timeout = _number(connect_text, 2.0)
    heartbeat_timeout = _number(heartbeat_text, 5.0)
    max_age = _number(age_text, 3600.0)
    idle = _number(idle_text, 120.0)
    # The token is an identification value visible in the process arguments;
    # it is deliberately never interpreted as code, a command, or an address.
    if not token or len(token) > 512 or "\x00" in token:
        raise ValueError
    return port, connect_timeout, heartbeat_timeout, max_age, idle


class Relay:
    def __init__(self, port, connect_timeout, heartbeat_timeout, max_age, idle):
        self.port = port
        self.connect_timeout = connect_timeout
        self.heartbeat_timeout = heartbeat_timeout
        self.max_age = max_age
        self.idle = idle
        self.selector = selectors.DefaultSelector()
        self.socket = None
        self.connected = False
        self.connecting = False
        self.stdin_open = True
        self.input = bytearray()
        self.request = bytearray()
        self.fin_requested = False
        self.fin_sent = False
        self.remote_eof = False
        self.eof_queued = False
        self.pending_credit = 0
        self.remote_uncredited = 0
        # Entries are [serialized frame, current offset, type, payload length].
        self.output = deque()
        self.output_bytes = 0
        self.output_data_bytes = 0
        self.output_controls = 0
        self.phase = "running"
        self.returncode = 0
        self.shutdown_deadline = None
        self.born = time.monotonic()
        self.connect_deadline = self.born + connect_timeout
        self.last_ping = self.born
        self.last_business = self.born
        self.done_queued = False
        self.running = True

    def _unregister(self, fileobj):
        try:
            self.selector.unregister(fileobj)
        except (KeyError, OSError, ValueError):
            pass

    def _close_socket(self):
        sock = self.socket
        self.socket = None
        self.connected = False
        self.connecting = False
        if sock is not None:
            self._unregister(sock)
            try:
                sock.close()
            except OSError:
                pass

    def _stop_immediately(self, code):
        if not self.running:
            return
        self.phase = "stopped"
        self.returncode = code
        self.running = False
        self._unregister(0)
        self._unregister(1)
        self._close_socket()
        self.output.clear()
        self.output_bytes = 0
        self.output_data_bytes = 0
        self.output_controls = 0

    def _begin_output_shutdown(self, code):
        if not self.running:
            return
        self.phase = "drain"
        self.returncode = code
        self.shutdown_deadline = time.monotonic() + _SHUTDOWN_SECONDS
        self._unregister(0)
        self._close_socket()
        if not self.output:
            self.running = False

    def _queue_control(self, kind, payload=b""):
        if len(payload) > _MAX_PAYLOAD:
            raise RuntimeError
        if self.output_controls >= _CONTROL_LIMIT:
            raise RuntimeError
        frame = _HEADER.pack(kind, len(payload)) + payload
        if self.output_bytes + len(frame) > _OUTPUT_LIMIT:
            raise RuntimeError
        self.output.append([frame, 0, kind, len(payload)])
        self.output_bytes += len(frame)
        self.output_controls += 1

    def _queue_data(self, payload):
        size = len(payload)
        if not 0 < size <= _MAX_PAYLOAD:
            raise RuntimeError
        if self.remote_uncredited + size > _WINDOW:
            raise RuntimeError
        frame = _HEADER.pack(_REMOTE_DATA, size) + payload
        if self.output_bytes + len(frame) > _OUTPUT_LIMIT:
            return False
        self.output.append([frame, 0, _REMOTE_DATA, size])
        self.output_bytes += len(frame)
        self.output_data_bytes += size
        self.remote_uncredited += size
        return True

    def _flush_credit(self):
        if not self.pending_credit:
            return
        amount = self.pending_credit
        if not 0 < amount <= _WINDOW:
            raise RuntimeError
        self._queue_control(_REMOTE_CREDIT, _COUNT.pack(amount))
        self.pending_credit = 0

    def _read_allowance(self):
        if not self.connected or self.remote_eof:
            return 0
        allowance = _WINDOW - self.remote_uncredited
        room = _OUTPUT_LIMIT - self.output_bytes - _HEADER.size
        if allowance <= 0 or room <= 0:
            return 0
        return min(_MAX_PAYLOAD, allowance, room)

    def _update_interest(self):
        if not self.running:
            return
        if self.stdin_open and self.phase == "running":
            try:
                self.selector.modify(0, selectors.EVENT_READ, "stdin")
            except KeyError:
                self.selector.register(0, selectors.EVENT_READ, "stdin")
        else:
            self._unregister(0)
        if self.output:
            try:
                self.selector.modify(1, selectors.EVENT_WRITE, "stdout")
            except KeyError:
                self.selector.register(1, selectors.EVENT_WRITE, "stdout")
        else:
            self._unregister(1)
        if self.socket is None:
            return
        if self.connecting:
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
        elif self.connected:
            events = 0
            if self.request or (self.fin_requested and not self.fin_sent):
                events |= selectors.EVENT_WRITE
            if self._read_allowance():
                events |= selectors.EVENT_READ
        else:
            events = 0
        if not events:
            self._unregister(self.socket)
            return
        try:
            self.selector.modify(self.socket, events, "socket")
        except KeyError:
            self.selector.register(self.socket, events, "socket")

    def _queue_eof(self):
        if self.eof_queued:
            return
        self._flush_credit()
        self._queue_control(_EOF)
        self.eof_queued = True

    def _shutdown_write(self):
        if self.fin_sent:
            return
        if self.socket is None or not self.connected:
            return
        try:
            self.socket.shutdown(socket.SHUT_WR)
        except OSError:
            self._stop_immediately(1)
            return
        self.fin_sent = True

    def _connect_succeeded(self):
        if not self.running or not self.connecting or self.socket is None:
            return
        self.connecting = False
        self.connected = True
        self._queue_control(_READY, _COUNT.pack(os.getpid()))
        if self.fin_requested and not self.request:
            self._shutdown_write()

    def _connect_failed(self):
        if not self.running or self.phase != "running":
            return
        self._close_socket()
        self._queue_control(_CONNECT_FAILED)
        self._begin_output_shutdown(0)

    def _finish_connect(self):
        if self.socket is None:
            return
        try:
            error = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        except OSError:
            self._connect_failed()
            return
        if error == 0:
            try:
                self._connect_succeeded()
            except BaseException:
                self._stop_immediately(1)
        else:
            self._connect_failed()

    def _handle_data(self, payload):
        if not payload or self.fin_requested:
            raise RuntimeError
        if len(self.request) + len(payload) > _WINDOW:
            raise RuntimeError
        self.request.extend(payload)
        self.last_business = time.monotonic()

    def _handle_frame(self, kind, payload):
        size = len(payload)
        if kind == _DATA:
            if size == 0:
                raise RuntimeError
            self._handle_data(payload)
        elif kind == _FIN:
            if size or self.fin_requested:
                raise RuntimeError
            self.fin_requested = True
            if self.connected and not self.request:
                self._shutdown_write()
        elif kind == _PING:
            if size:
                raise RuntimeError
            self.last_ping = time.monotonic()
        elif kind == _CLOSE:
            if size:
                raise RuntimeError
            self._stop_immediately(0)
        elif kind == _CREDIT:
            if size != _COUNT.size:
                raise RuntimeError
            amount = _COUNT.unpack(payload)[0]
            if not amount or amount > self.remote_uncredited:
                raise RuntimeError
            self.remote_uncredited -= amount
        else:
            raise RuntimeError

    def _read_stdin(self):
        try:
            chunk = os.read(0, _MAX_PAYLOAD)
        except BlockingIOError:
            return
        except OSError:
            self._stop_immediately(1)
            return
        if not chunk:
            if self.input:
                self._stop_immediately(1)
            else:
                self._stop_immediately(0)
            return
        self.input.extend(chunk)
        # One read can complete a large frame and contain the beginning of the
        # next one.  Parse complete frames before applying the residual bound.
        if len(self.input) > 2 * _MAX_PAYLOAD + _HEADER.size:
            self._stop_immediately(1)
            return
        while self.running and self.phase == "running":
            if len(self.input) < _HEADER.size:
                break
            kind, size = _HEADER.unpack(self.input[:_HEADER.size])
            if size > _MAX_PAYLOAD:
                self._stop_immediately(1)
                return
            total = _HEADER.size + size
            if len(self.input) < total:
                break
            payload = bytes(self.input[_HEADER.size:total])
            del self.input[:total]
            try:
                self._handle_frame(kind, payload)
            except (RuntimeError, ValueError, OSError):
                self._stop_immediately(1)
                return
        if self.running and len(self.input) > _MAX_PAYLOAD + _HEADER.size:
            self._stop_immediately(1)

    def _write_tcp(self):
        if self.socket is None or not self.connected:
            return
        if self.request:
            try:
                count = self.socket.send(bytes(self.request[:_MAX_PAYLOAD]))
            except BlockingIOError:
                return
            except OSError:
                self._stop_immediately(1)
                return
            if count <= 0:
                self._stop_immediately(1)
                return
            del self.request[:count]
            self.pending_credit += count
            if self.pending_credit > _WINDOW:
                self._stop_immediately(1)
                return
            self.last_business = time.monotonic()
        if self.connected and self.fin_requested and not self.request and not self.fin_sent:
            self._shutdown_write()

    def _read_tcp(self):
        if self.socket is None or not self.connected:
            return
        allowance = self._read_allowance()
        if not allowance:
            return
        try:
            payload = self.socket.recv(allowance)
        except BlockingIOError:
            return
        except OSError:
            self._stop_immediately(1)
            return
        if payload:
            if not self._queue_data(payload):
                self._stop_immediately(1)
                return
            self.last_business = time.monotonic()
        else:
            self.remote_eof = True
            try:
                self._queue_eof()
            except (RuntimeError, ValueError):
                self._stop_immediately(1)

    def _write_stdout(self):
        while self.output and self.running:
            entry = self.output[0]
            frame, offset, kind, payload_size = entry
            try:
                count = os.write(1, frame[offset:])
            except BlockingIOError:
                return
            except (BrokenPipeError, OSError):
                self._stop_immediately(1)
                return
            if count <= 0:
                self._stop_immediately(1)
                return
            offset += count
            self.output_bytes -= count
            entry[1] = offset
            if offset == len(frame):
                self.output.popleft()
                if kind == _REMOTE_DATA:
                    self.output_data_bytes -= payload_size
                else:
                    self.output_controls -= 1
        if self.phase == "drain" and not self.output:
            self.running = False

    def _maybe_done(self):
        if not self.running or self.phase != "running" or self.done_queued:
            return
        # The two FINs are independent.  The request FIN is the TCP write
        # half-close, while remote_eof is the TCP read half-close.
        if not (self.fin_sent and self.remote_eof and not self.request):
            return
        # Host must acknowledge every response DATA before normal completion.
        # This is intentionally checked separately from queue draining: the
        # final CREDIT may arrive after the host wrote DATA to its socketpair.
        if self.remote_uncredited:
            if self.shutdown_deadline is None:
                self.shutdown_deadline = time.monotonic() + _SHUTDOWN_SECONDS
            return
        if self.output:
            if self.shutdown_deadline is None:
                self.shutdown_deadline = time.monotonic() + _SHUTDOWN_SECONDS
            return
        self._flush_credit()
        if self.output or self.pending_credit:
            if self.shutdown_deadline is None:
                self.shutdown_deadline = time.monotonic() + _SHUTDOWN_SECONDS
            return
        self._queue_control(_DONE)
        self.done_queued = True
        self._begin_output_shutdown(0)

    def _expire(self):
        if not self.running:
            return
        now = time.monotonic()
        if self.phase == "drain":
            if not self.output or (self.shutdown_deadline is not None
                                   and now >= self.shutdown_deadline):
                self.running = False
            return
        if now >= self.born + self.max_age:
            self._stop_immediately(1)
            return
        if now >= self.last_ping + self.heartbeat_timeout:
            self._stop_immediately(1)
            return
        if now >= self.last_business + self.idle:
            self._stop_immediately(1)
            return
        if self.connecting and now >= self.connect_deadline:
            self._connect_failed()

    def _timeout(self):
        now = time.monotonic()
        deadlines = [self.born + self.max_age,
                     self.last_ping + self.heartbeat_timeout,
                     self.last_business + self.idle]
        if self.connecting:
            deadlines.append(self.connect_deadline)
        if self.phase == "drain" and self.shutdown_deadline is not None:
            deadlines.append(self.shutdown_deadline)
        remaining = min(deadlines) - now
        return max(0.0, remaining)

    def _open(self):
        os.set_blocking(0, False)
        os.set_blocking(1, False)
        if os.isatty(0) or os.isatty(1):
            raise ValueError
        self.selector.register(0, selectors.EVENT_READ, "stdin")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setblocking(False)
        result = self.socket.connect_ex(("127.0.0.1", self.port))
        if result == 0:
            self.connecting = True
            self._connect_succeeded()
        elif result in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK,
                        errno.EINTR):
            self.connecting = True
            self.selector.register(self.socket,
                                   selectors.EVENT_READ | selectors.EVENT_WRITE,
                                   "socket")
        else:
            self._connect_failed()

    def run(self):
        try:
            self._open()
            while self.running:
                self._expire()
                if not self.running:
                    break
                try:
                    self._flush_credit()
                    self._maybe_done()
                except (RuntimeError, ValueError):
                    self._stop_immediately(1)
                    break
                if not self.running:
                    break
                self._update_interest()
                events = self.selector.select(self._timeout())
                # Process control input before output/socket work so PING is
                # serviced even when stdout or TCP is under sustained pressure.
                events.sort(key=lambda item: 0 if item[0].data == "stdin" else 1)
                for key, mask in events:
                    if not self.running:
                        break
                    if key.data == "stdin":
                        self._read_stdin()
                    elif key.data == "stdout":
                        self._write_stdout()
                    elif key.data == "socket":
                        if self.connecting:
                            self._finish_connect()
                        elif self.connected:
                            if mask & selectors.EVENT_WRITE:
                                self._write_tcp()
                            if self.running and mask & selectors.EVENT_READ:
                                self._read_tcp()
                if not self.running:
                    break
                try:
                    self._flush_credit()
                    self._maybe_done()
                except (RuntimeError, ValueError):
                    self._stop_immediately(1)
            return self.returncode
        finally:
            self._close_socket()
            try:
                self.selector.close()
            except OSError:
                pass


def main(argv):
    try:
        port, connect_timeout, heartbeat_timeout, max_age, idle = _arguments(argv)
        relay = Relay(port, connect_timeout, heartbeat_timeout, max_age, idle)
        return relay.run()
    except ValueError:
        _stderr(_ERR_INVALID_ARGUMENTS)
        return 2
    except (RuntimeError, OSError, selectors.error):
        _stderr(_ERR_RUNTIME)
        return 1
    except BaseException:
        _stderr(_ERR_RUNTIME)
        return 1


try:
    _exit_code = main(sys.argv[1:])
except BaseException:
    _stderr(_ERR_RUNTIME)
    _exit_code = 1
sys.exit(_exit_code)
'''

__all__ = ["RELAY_CODE"]
