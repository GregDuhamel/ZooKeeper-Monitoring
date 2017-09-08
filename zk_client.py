#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import logging

from optparse import OptionParser
from kazoo.client import KazooClient

__version__ = (0, 1, 0)

# création de l'objet logger qui va nous servir à écrire dans les logs
logger = logging.getLogger()
# on met le niveau du logger à DEBUG, comme ça il écrit tout
logger.setLevel(logging.DEBUG)
# création d'un formateur qui va ajouter le temps, le niveau
# de chaque message quand on écrira un message dans le log
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
# création d'un second handler qui va rediriger chaque écriture de log
# sur la console
stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class ZKClient:
    def __init__(self, hosts="localhost:10000", timeout=5):
        """
        initialise Class ZooClient
        :type timeout: object
        """
        self.zk = KazooClient(hosts)

    def connect(self):
        self.zk.start(timeout=5)

    def disconnect(self):
        self.zk.stop()

    def createZnode(self, path):
        if (self.zk.exists(path)):
            logger.error("path %s already exists. Exiting.", path)
            exit(1)
        if (self.zk.ensure_path(path)):
            return 0

def get_version():
    return '.'.join(map(str, __version__))

    # if zk.exists("/MDW/monitor"):
    #     logger.warning("zkNode /MDW/monitor already exists.")
    #     data, stat = zk.get("/MDW/monitor")
    #     logger.warning("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    #     os.getpid()


def parse_cli():
    parser = OptionParser(usage='./zk_client_ckeck.py <options>', version=get_version())

    parser.add_option('-s', '--servers', dest='servers', help='a list of ZooKeeper servers', metavar='sever1:port1,server:port2')
    parser.add_option('-t', '--timeout', dest='timeout', help='maximum number of second to connect ZooKeeper quorum', metavar='10')

    opts, args = parser.parse_args()

    if opts.servers is None:
        parser.error('The list of servers is mandatory')

    return opts, args


def main():
    opts, args = parse_cli()

    zkc = ZKClient(opts.servers, opts.timeout)

    zkc.connect()

    zkc.createZnode("MDW/monitor    ")

    zkc.disconnect()

if __name__ == '__main__':
    sys.exit(main())
