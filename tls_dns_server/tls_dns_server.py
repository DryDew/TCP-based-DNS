# encoding = utf-8
# author: Wei Dai
# date: 11/27/2018
"""
# file_name: tls_dns_server.py
# description:
#   1. This a Python script for query IP address of a domain name, playing a role of TLS DNS server but it is only a template.
#   2. When the server start, it will read through a database file (*.dat containing all query about one TLS domain) and
    print "server start".
#   3. When new client connects to the server, it will print "accept {ip_address}, {port}" and folk a thread to handle
    it.
#   4. For resolve query, no matter what the method is, it will check database and give a response to the sender.
#   5.  When connection between TSL server and any senders ends abnormally, it will print "Loss connection
    {ip_address}, {port}".
"""

import socket


class DNSTLSServer:

    def __init__(self, id_, port_, default_file):
        address = ('127.0.0.1', port_)

        self.id = id_
        self.sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sk.bind(address)
        self.sk.listen(5)

        self.msg_size = 64 * 1024

        self.dns_database = self.build_database(default_file)

        self.log_dir = './log/{0}.log'.format(self.id)

    @staticmethod
    def build_database(file):
        cache = {}
        with open(file) as f:
            data = f.readlines()
        for line in data:
            line_list = line.strip().split()
            cache[line_list[0].lower()] = line_list[1]
        return cache

    def accept(self):
        return self.sk.accept()

    def write_log(self, msg):
        with open(self.log_dir, 'a', encoding='utf-8') as f:
            f.write(msg)

    def recv_query(self, connection_):
        try:
            data = connection_.recv(self.msg_size)
        except ConnectionResetError:
            connection_.close()
            return ''

        query_ = str(data, encoding='utf-8')

        self.write_log(query_[1:-1] + '\n')
        return query_

    def cache_query(self, domain):
        domain_list = domain.split('.')
        if domain_list[0] != 'www':
            q1 = '.'.join(['www'] + domain_list)
            q2 = domain
        else:
            q1 = domain
            q2 = '.'.join(domain_list[1:])
        result1 = self.dns_database.get(q1, '')
        result2 = self.dns_database.get(q2, '')
        if result1 == '':
            return result2
        else:
            return result1

    def resolve_query(self, query, connection, address):
        query_list = query[1:-1].split(',')
        if len(query_list) != 3:
            send_msg = "<0xEE, {0}, {1}>".format(self.id, "Invalid format")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
            return None
        domain = query_list[1].strip()
        method = query_list[2].strip()

        if method != 'R' and method != 'I':
            send_msg = "<0xEE, {0}, {1}>".format(self.id, "Invalid format")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
            return None

        result = self.cache_query(domain)

        if result != '':
            send_msg = "<0x00, {0}, {1}>".format(self.id, result)
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')

        else:
            send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
