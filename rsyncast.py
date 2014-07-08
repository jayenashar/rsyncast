#!/usr/bin/python
from __future__ import print_function
from argparse import ArgumentParser
from atexit import register
from os import close, remove
from shlex import split
from signal import SIGINT
from subprocess import Popen, check_call, PIPE
from sys import stderr
from tempfile import mkstemp
from termios import tcgetattr, tcsetattr, TCSANOW
from time import sleep

SUBNET='10.0.0.0/8'

parser = ArgumentParser(description='Multicast wrapper for rsync.  Does not '
                                    'require destinations to be identical.  '
                                    'Utilizes rsync protocol.')
parser.add_argument('--udp-sender-args',
                    help='Arguments to pass to udp-sender (e.g., '
                         '--full-duplex --broadcast)')
parser.add_argument('--rsync-args',
                    help='Arguments to pass to rsync (e.g., -a --protocol=30)')
parser.add_argument('--write-batch', action='store_true',
                    help='sends data while calculating what to send')
parser.add_argument('--pipe',
                    help='compresses data with PIPE.  Assumes PIPE -d to '
                         'decompress (e.g., gzip)')
parser.add_argument('SRC')
parser.add_argument('DEST', nargs='+')

args = parser.parse_args()


def interrupt_if_not_pollable(subprocess):
    if subprocess.poll() is None:
        print(subprocess.pid, "should have terminated.  terminating...",
              file=stderr)
        subprocess.send_signal(SIGINT)


def restore_terminal():
    for fd in range(3):
        tcsetattr(fd, TCSANOW, tcs[fd])


# ssh? screws up the terminal, so save its attributes and restore them later
tcs = {}
for fd in range(3):
    tcs[fd] = tcgetattr(fd)
register(restore_terminal)

# udp-receiver really needs the interface
interfaces = {}
for dest in args.DEST:
    dest_host = dest.split(':')[0]
    ip_cmd = ['ssh', '-n', dest_host, 'ip -4 route']
    for line in Popen(ip_cmd, stdout=PIPE).communicate()[0].splitlines():
        if line.startswith(SUBNET):
            interfaces[dest_host] = line.split()[2]

for dest_index in range(len(args.DEST)):
    (temp_fd, temp_filename) = mkstemp(suffix='rsync',
                                       prefix='dest' + str(dest_index))
    close(temp_fd)

    if args.write_batch:
        batch_arg_key = '--write-batch='
        udp_dests = args.DEST[dest_index + 1:]
    else:
        batch_arg_key = '--only-write-batch='
        udp_dests = args.DEST[dest_index:]

    rsync_cmd = ['rsync', batch_arg_key + temp_filename]
    rsync_cmd += split(args.rsync_args) + [args.SRC, args.DEST[dest_index]]
    check_call(rsync_cmd)
    register(remove, temp_filename)
    register(remove, temp_filename + '.sh')
    if len(udp_dests) > 0:
        # --nokbd otherwise sending starts on the first receiver only
        udp_receiver_cmd = 'udp-receiver --nokbd'
        udp_sender_cmd = ['udp-sender', '--min-receivers',
                          str(len(udp_dests)), '--file', temp_filename,
                          '--start-timeout', '1', '--rexmit-hello-interval',
                          '100'] + args.udp_sender_args.split()
        if args.pipe:
            udp_receiver_cmd += ' --pipe "' + args.pipe + ' -d"'
            udp_sender_cmd += ['--pipe', args.pipe]
        udp_receiver_subprocesses = []
        for udp_dest in udp_dests:
            [udp_dest_host, udp_dest_path] = udp_dest.split(':')
            if udp_dest_path == '':
                udp_dest_path = '.'
            udp_receiver_cmd += ' --interface ' + interfaces[udp_dest_host]
            rsync_receiver_cmd = 'rsync --read-batch=- ' + args.rsync_args + \
                                 ' ' + udp_dest_path
            # -t so interrupting ssh will interrupt the remote shell
            # -n because we should suppress stdin
            # -t again to force psuedo-tty
            udp_receiver_subprocess = Popen(['ssh', '-tnt', udp_dest_host,
                                             udp_receiver_cmd + '|' +
                                             rsync_receiver_cmd])
            udp_receiver_subprocesses.append(udp_receiver_subprocess)
            register(interrupt_if_not_pollable, udp_receiver_subprocess)
        sleep(1) # wait for receivers to start or bail
        restore_terminal()
        for udp_receiver_subprocess in udp_receiver_subprocesses:
            if udp_receiver_subprocess.poll() is not None:
                print(udp_receiver_subprocess.pid,
                      "should not have terminated.  terminating...",
                      file=stderr)
                exit(59)
        check_call(udp_sender_cmd)
        sleep(1) # wait for receivers to exit
        for udp_receiver_subprocess in udp_receiver_subprocesses:
            interrupt_if_not_pollable(udp_receiver_subprocess)
