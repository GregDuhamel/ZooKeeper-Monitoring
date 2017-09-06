#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import socket
import logging
import re
import io

from optparse import OptionParser, OptionGroup
from logging.handlers import RotatingFileHandler

__version__ = (0, 1, 0)

# création de l'objet logger qui va nous servir à écrire dans les logs
logger = logging.getLogger()
# on met le niveau du logger à DEBUG, comme ça il écrit tout
logger.setLevel(logging.DEBUG)
# création d'un formateur qui va ajouter le temps, le niveau
# de chaque message quand on écrira un message dans le log
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
# création d'un handler qui va rediriger une écriture du log vers
# un fichier en mode 'append', avec 1 backup et une taille max de 1Mo
# noinspection PyArgumentEqualDefault
file_handler = RotatingFileHandler('/tmp/check_zookeeper.log', 'a', 1000000, 1)
# on lui met le niveau sur DEBUG, on lui dit qu'il doit utiliser le formateur
# créé précédement et on ajoute ce handler au logger
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
# création d'un second handler qui va rediriger chaque écriture de log
# sur la console
stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)


class NagiosHandler():
    @classmethod
    def register_options(cls, parser):
        group = OptionGroup(parser, 'Nagios specific options')

        group.add_option('-w', '--warning', dest='warning')
        group.add_option('-c', '--critical', dest='critical')

        parser.add_option_group(group)

    @staticmethod
    def analyze(opts, cluster_stats):
        try:
            warning = int(opts.warning)
            critical = int(opts.critical)

        except (TypeError, ValueError):
            logger.error('Invalid values for "warning" and "critical"')
            return 2

        if opts.key is None:
            logger.error('You should specify a key name.')
            return 2

        warning_state, critical_state, values = [], [], []
        for host, stats in cluster_stats.items():
            if opts.key in stats:

                value = stats[opts.key]
                values.append('%s' % value)

                if warning >= value > critical or warning <= value < critical:
                    warning_state.append(host)

                elif (warning < critical <= value) or (warning > critical >= value):
                    critical_state.append(host)

        if not values:
            # Zookeeper may be down, not serving requests or we may have a bad configuration
            logger.error('Critical, %s not found', opts.key)
            return 2

        values = ' '.join(values)
        if critical_state:
            logger.error('CRITICAL - "%s" %s : %s' % (opts.key, ', '.join(critical_state), values))
            return 2
        elif warning_state:
            logger.warning('WARNING - "%s" %s : %s' % (opts.key, ', '.join(warning_state), values))
            return 1
        else:
            logger.info('OK - "%s" : %s' % (opts.key, values))
            return 0


class ZooKeeperServer():
    def __init__(self, host='localhost', port='2181', timeout=1):
        self._address = (host, int(port))
        self._timeout = timeout

    def get_stats(self):
        """ Get ZooKeeper server stats as a map """
        data = self._send_cmd('mntr')
        if data:
            return self._parse(data)
        else:
            data = self._send_cmd('stat')
            return self._parse_stat(data)

    @staticmethod
    def _create_socket():
        return socket.socket()

    def _send_cmd(self, cmd):
        """ Send a 4letter word command to the server """
        s = self._create_socket()
        s.settimeout(self._timeout)

        s.connect(self._address)
        buf = cmd.encode()
        s.send(buf)

        data = s.recv(2048)
        s.close()

        return data

    def _parse(self, data):
        """ Parse the output from the 'mntr' 4letter word command """
        h = io.StringIO(data.decode())
        result = {}
        for line in h.readlines():
            try:
                key, value = self._parse_line(line)
                result[key] = value
            except ValueError:
                pass  # ignore broken lines

        return result

    @staticmethod
    def _parse_stat(data):
        """ Parse the output from the 'stat' 4letter word command """
        h = io.StringIO(data.decode())

        result = {}
        version = h.readline()
        if version:
            result['zk_version'] = version[version.index(':') + 1:].strip()

        # skip all lines until we find the empty one
        while h.readline().strip():
            pass

        for line in h.readlines():
            m = re.match('Latency min/avg/max: (\d+)/(\d+)/(\d+)', line)
            if m is not None:
                result['zk_min_latency'] = int(m.group(1))
                result['zk_avg_latency'] = int(m.group(2))
                result['zk_max_latency'] = int(m.group(3))
                continue

            m = re.match('Received: (\d+)', line)
            if m is not None:
                result['zk_packets_received'] = int(m.group(1))
                continue

            m = re.match('Sent: (\d+)', line)
            if m is not None:
                result['zk_packets_sent'] = int(m.group(1))
                continue

            m = re.match('Outstanding: (\d+)', line)
            if m is not None:
                result['zk_outstanding_requests'] = int(m.group(1))
                continue

            m = re.match('Mode: (.*)', line)
            if m is not None:
                result['zk_server_state'] = m.group(1)
                continue

            m = re.match('Node count: (\d+)', line)
            if m is not None:
                result['zk_znode_count'] = int(m.group(1))
                continue

        return result

    @staticmethod
    def _parse_line(line):
        try:
            key, value = map(str.strip, line.split('\t'))
        except ValueError:
            raise ValueError('Found invalid line: %s' % line)

        if not key:
            raise ValueError('The key is mandatory and should not be empty')

        try:
            value = int(value)
        except (TypeError, ValueError):
            pass

        return key, value


def main():
    opts, args = parse_cli()

    cluster_stats = get_cluster_stats(opts.servers)
    if opts.output is None:
        dump_stats(cluster_stats)
        return 0

    handler = create_handler(opts.output)
    if handler is None:
        logger.error('undefined handler: %s' % opts.output)
        sys.exit(1)

    return handler.analyze(opts, cluster_stats)


def create_handler(name):
    """ Return an instance of a platform specific analyzer """
    try:
        return globals()['%sHandler' % name.capitalize()]()
    except KeyError:
        return None


def get_all_handlers():
    """ Get a list containing all the platform specific analyzers """
    return [NagiosHandler]


def dump_stats(cluster_stats):
    """ Dump cluster statistics in an user friendly format """
    for server, stats in cluster_stats.items():
        print('Server:', server)

        for key, value in stats.items():
            print("%30s" % key, ' ', value)
        print()


def get_cluster_stats(servers):
    """ Get stats for all the servers in the cluster """
    stats = {}
    for host, port in servers:
        try:
            zk = ZooKeeperServer(host, port)
            stats["%s:%s" % (host, port)] = zk.get_stats()

        except socket.error as e:
            # ignore because the cluster can still work even
            # if some servers fail completely

            # this error should be also visible in a variable
            # exposed by the server in the statistics

            logger.info('unable to connect to server "%s" on port "%s" due to "%s"' % (host, port, e))

    return stats


def get_version():
    return '.'.join(map(str, __version__))


def parse_cli():
    parser = OptionParser(usage='./check_zookeeper.py <options>', version=get_version())

    parser.add_option('-s', '--servers', dest='servers', help='a list of SERVERS', metavar='SERVERS')

    parser.add_option('-o', '--output', dest='output', help='output HANDLER: nagios', metavar='HANDLER')

    parser.add_option('-k', '--key', dest='key')

    for handler in get_all_handlers():
        handler.register_options(parser)

    opts, args = parser.parse_args()

    if opts.servers is None:
        parser.error('The list of servers is mandatory')

    opts.servers = [s.split(':') for s in opts.servers.split(',')]

    logger.debug("Actual list of server is %s", opts.servers)

    return opts, args


if __name__ == '__main__':
    sys.exit(main())
