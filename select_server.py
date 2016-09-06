#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import socket
import select
import argparse
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


EVENT_MAP = {
    1: 'POLLIN (There is data to read)',
    2: 'POLLPRI (There is urgent data to read)',
    4: 'POLLOUT (Ready for output: writing will not block)',
    8: 'POLLERR (Error condition of some sort)',
    16: 'POLLHUP (Hung up)',
    19: 'POLLHUP (Hung up)',
    32: 'POLLNVAL (Invalid request: descriptor not open)',
}


def parse_args(args=None):
    """
    Parse command line args

    :param list args: (Optional) Command line arguments (Default: `sys.argv`)
    :returns: Argument namespace
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--execute', dest='COMMAND', nargs=argparse.REMAINDER,
                        help='Pipe output to COMMAND - Must be last arg (Default: stdout)')
    parser.add_argument('-b', '--buffer-size', default=1024,  # 4096 ?
                        dest='BUFF_SIZE', help='Read buffer size (Default: %(default)s)', type=int)
    parser.add_argument('-i', '--interface', default='0.0.0.0', dest='INTERFACE',
                        help='Interface/IP to listen on (Default: %(default)s)')
    parser.add_argument('-p', '--port', default=514, dest='PORT',
                        help='Port to listen on (Default: %(default)s)', type=int)
    parser.add_argument('-v', '--verbose', action='count', default=0, dest='VERBOSE',
                        help='Verbose output (multiple times [max 3] for increasing verbosity)')
    return parser.parse_args(args)


def main():
    """
    Listen for all TCP connections on a single port and write all revieved data to stdout or
    pipe it to the stdin of the command provided via the `-e` arg.
    """
    args = parse_args()
    connections = {}  # mapping of socket fileno's to socket objects
    buff_size = args.BUFF_SIZE
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((args.INTERFACE, args.PORT))
    server_socket.listen(10)
    server_sock_fd = server_socket.fileno()
    wait = 0.05
    reg_events = select.POLLIN | select.POLLPRI | select.POLLERR | select.POLLHUP | select.POLLNVAL
    poll = select.poll()
    poll.register(server_sock_fd, reg_events)
    if args.COMMAND:
        sub_proc = subprocess.Popen(args.COMMAND, stdin=subprocess.PIPE)
        p_stdin = sub_proc.stdin
    else:
        sub_proc = None
        p_stdin = sys.stdout
    try:
        while True:
            # Get the list sockets which are ready to be read through select
            read_sockets = poll.poll(5)
            for (sock_fd, event) in read_sockets:
                if args.VERBOSE > 1:
                    event_str = EVENT_MAP.get(event)
                    if event_str is None:
                        event_str = str(event)
                    sys.stderr.write(
                        'Socket: {:<3} -- Event: {}\n'.format(sock_fd, event_str))

                if sock_fd == server_sock_fd:
                    # New connection
                    if event & select.POLLIN == event or event & select.POLLPRI == event:
                        # New connection recieved through server_socket
                        conn, addr = server_socket.accept()
                        conn_fd = conn.fileno()
                        connections[conn_fd] = conn
                        poll.register(conn_fd, reg_events)
                        if args.VERBOSE:
                            sys.stderr.write('New connection from {} [{}]\n'.format(addr, conn_fd))
                    else:
                        err_msg = ''
                        pn = server_socket.getpeername()
                        if event & select.POLLNVAL:
                            err_msg = 'Invalid request: descriptor not open: '
                        elif event & select.POLLHUP:
                            err_msg = 'Hung up: '
                        elif event & select.POLLERR:
                            err_msg = 'Connection Error: '
                        err_msg += '{} [{}]\n'.format(pn, sock_fd)
                        if args.VERBOSE:
                            sys.stderr.write(err_msg)
                        err = socket.error(err_msg)
                        err.errno = event
                        raise err
                else:
                    # Process incoming message from client
                    conn = connections.get(sock_fd)
                    if conn is not None and (event & select.POLLIN == event or
                                             event & select.POLLPRI == event):
                        try:
                            # Write the client message
                            data = conn.recv(buff_size)
                            if not data:
                                continue
                            p_stdin.write(data)
                            while not data.endswith('\n'):
                                if args.VERBOSE > 2:
                                    sys.stderr.write(
                                        '[{}] Checking for more input...\n'.format(sock_fd))
                                    sys.stderr.flush()
                                r_conn, _, _ = select.select([conn], [], [], wait)
                                if not r_conn:
                                    if args.VERBOSE > 2:
                                        sys.stderr.write('[{}] No more input\n'.format(sock_fd))
                                        sys.stderr.flush()
                                    break
                                if args.VERBOSE > 2:
                                    sys.stderr.write('[{}] Found more input\n'.format(sock_fd))
                                    sys.stderr.flush()
                                data = conn.recv(buff_size)
                                if not data:
                                    break
                                p_stdin.write(data)
                            p_stdin.flush()
                        except socket.error:
                            # client disconnected, remove from socket list
                            if args.VERBOSE:
                                sys.stderr.write(
                                    'Closed conn: {} [{}]\n'.format(conn.getpeername(), sock_fd))
                            try:
                                conn.close()
                            except socket.error:
                                pass
                            poll.unregister(sock_fd)
                            del connections[sock_fd]
                    elif conn is None:
                        # This fd is not in our connections dict (possible?)
                        if args.VERBOSE:
                            sys.stderr.write('Error: Bad socket fd: {}\n'.format(sock_fd))
                        poll.unregister(sock_fd)
                    else:
                        # Handle error event
                        if args.VERBOSE:
                            pn = conn.getpeername()
                            if event & select.POLLNVAL:
                                sys.stderr.write('Invalid request: descriptor not open: ')
                            elif event & select.POLLHUP:
                                sys.stderr.write('Hung up: ')
                            elif event & select.POLLERR:
                                sys.stderr.write('Connection Error: ')
                            sys.stderr.write('{} [{}]\n'.format(pn, sock_fd))
                        poll.unregister(sock_fd)
                        del connections[sock_fd]
            else:
                time.sleep(wait)
    finally:
        try:
            poll.unregister(server_sock_fd)
            server_socket.close()
        except:
            pass
        if sub_proc is not None:
            try:
                sub_proc.terminate()
            except:
                pass

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print
