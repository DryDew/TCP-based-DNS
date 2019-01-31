# encoding = utf-8
# author: Wei Dai
# date: 11/27/2018
"""
# file_name: tls_gov.py
# description:
#   This is an entity of DNSTLSServer class from tls_dns_server.py, which handles .gov domain name query for DNS server.
    The server listen on address (127.0.0.1, 5680). (port: 5680)
"""

import socket
import threading

from tls_dns_server import DNSTLSServer


def process_connection(server, connection):
    while True:
        query = server.recv_query(connection)
        if query == '':
            print('Loss connection: {0}, {1}'.format(address[0], address[1]))
            break
        server.resolve_query(query, connection, address)
        connection.close()

        server.write_log('\n')
        break


gov_server = DNSTLSServer("GOV_DNS_Server", 5680, './data/gov.dat')
print("server start!")

while True:
    connection, address = gov_server.accept()
    print('accept: {0}, {1}'.format(address[0], address[1]))
    connection_thread = threading.Thread(target=process_connection, args=(gov_server, connection))
    connection_thread.daemon = False
    connection_thread.start()

gov_server.close()

