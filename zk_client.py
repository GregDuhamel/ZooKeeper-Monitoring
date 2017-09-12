#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import logging
import traceback

from optparse import OptionParser

from kazoo.handlers.threading import KazooTimeoutError
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, NoChildrenForEphemeralsError, ZookeeperError, BadVersionError

__version__ = (0, 1, 0)

# création de l'objet logger qui va nous servir à écrire dans les logs
logger = logging.getLogger()
# on met le niveau du logger à DEBUG, comme ça il écrit tout
logger.setLevel(logging.ERROR)
# création d'un formateur qui va ajouter le temps, le niveau
# de chaque message quand on écrira un message dans le log
formatter = logging.Formatter('%(levelname)s - %(message)s')
# création d'un second handler qui va rediriger chaque écriture de log
# sur la console
stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.ERROR)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class ZKClient:
    def __init__(self, hosts=None, timeout=None, path=None):
        """
        initialise Class ZooClient
        :type timeout: object
        """
        if hosts is None:
            self.hosts = "localhost:10000"
        else:
            self.hosts = hosts

        if path is None:
            self.path = "MDW/monitor"
        else:
            self.path = path

        if timeout is None:
            self.timeout = 30
        else:
            self.timeout = timeout

        self.getvalue = None
        self.zk = KazooClient(hosts=self.hosts, timeout=self.timeout, randomize_hosts=True, logger=logger)

    def connect(self):
        try:
            self.zk.start(timeout=self.timeout)
        except KazooTimeoutError:
            logger.error("Receive timeout during connection to %s", self.hosts)
            sys.exit(2)

    def disconnect(self):
        self.zk.stop()

    def createznode(self):
        try:
            self.path = self.zk.create(path=self.path, value=bytes(2), ephemeral=True, sequence=True, makepath=True)
        except NodeExistsError:
            logger.error('path %s already exists. Exiting.', self.path)
            sys.exit(2)
        except NoNodeError:
            logger.error("Can't find parent node in %s", self.path)
            sys.exit(2)
        except NoChildrenForEphemeralsError:
            logger.error("Can't create children on Ephemeral node. Please review your path %s", self.path)
            sys.exit(2)
        except ZookeeperError:
            logger.error(traceback.print_exc())
            sys.exit(2)

    def get(self):
        try:
            self.getvalue = self.zk.get(path=self.path)
        except NoNodeError:
            logger.error("path %s doesn't exists.", self.path)
            sys.exit(2)
        except ZookeeperError:
            logger.error(traceback.print_exc())
            sys.exit(2)

        if self.getvalue[0].decode('utf-8') != 'OK':
            logger.error("Can't retrieve data or data has not expected value %s", self.getvalue[0].decode('utf-8'))
            sys.exit(2)

    def set(self):
        try:
            self.zk.set(path=self.path, value='OK'.encode())
        except BadVersionError:
            logger.error("Version mismatch while writing OK to znode %s", self.path)
            sys.exit(2)
        except NoNodeError:
            logger.error("path %s doesn't exists.", self.path)
            sys.exit(2)
        except ZookeeperError:
            logger.error(traceback.print_exc())
            sys.exit(2)


def get_version():
    return '.'.join(map(str, __version__))


def parse_cli():
    parser = OptionParser(usage='./zk_client_ckeck.py <options>', version=get_version())

    parser.add_option('-s', '--servers', dest='servers', help='a list of ZooKeeper servers',
                      metavar='sever1:port1,server:port2')
    parser.add_option('-t', '--timeout', dest='timeout', help='maximum number of second to connect ZooKeeper quorum',
                      metavar='10', type=int)
    parser.add_option('-z', '--znode', dest='path', help='znode that will be created to monitor zookeeper (ephemeral)',
                      metavar='MDW/monitor')

    opts, args = parser.parse_args()

    return opts, args


def main():
    opts, args = parse_cli()

    zkc = ZKClient(opts.servers, opts.timeout, opts.path)

    zkc.connect()

    zkc.createznode()

    zkc.set()

    zkc.get()

    zkc.disconnect()

    print("OK - SUCCESSFULLY WRITE in :", zkc.hosts, file=sys.stdout, flush=True)


if __name__ == '__main__':
    sys.exit(main())
