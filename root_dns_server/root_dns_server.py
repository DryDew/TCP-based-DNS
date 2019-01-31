# encoding = utf-8
# author: Wei Dai
# date: 11/27/2018
"""
# file_name: root_dns_server.py
# description:
#   1. This a Python script for query IP address of a domain name, playing a role of DNS default local server.
    The server listen on address (127.0.0.1, 5353). (port: 5353)
#   2. When the server start, it will read through a server file (server.dat containing TLS address) and print "server
    start".
#   3. When new client connects to the server, it will print "accept {ip_address}, {port}" and folk a thread to handle it.
#   4. For resolve query, no matter what the method is, it will check the suffix of the domain name and find out next
    query address. (TLS address)
    - 4.1. If the method is recursive (R),it will establish a new connection to the next query address and make a query
      on behalf of user. The result returned by TLS DNS server is the final answer. Then, send a response to default
      local sever.
    - 4.2. If the method is iterative (I), root DNS server will send next query address back to default local sever.
#    5. If any of the server that root DNS requests break down or loss connection or time out, the root DNS server will
       send <0xFF, {id}, "Host not found"> back to default local DNS server.
"""


import time
import socket
import threading


class DNSRootServer:

    def __init__(self, id_, port_, server_file):
        address_ = ('127.0.0.1', port_)

        self.id = id_
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(address_)
        self.server_socket.listen(5)

        self.msg_size = 64 * 1024

        self.dns_server_dict = self.build_default_server_dict(server_file)
        # self.dns_server_dict = {'com': ('127.0.0.1', 5678),
        #                         'org': ('127.0.0.1', 5679),
        #                         'gov': ('127.0.0.1', 5680),
        #                         }
        self.log_dir = './log/{0}.log'.format(self.id)

    @staticmethod
    def build_default_server_dict(file):
        cache = {}
        with open(file) as f:
            data = f.readlines()
        for line in data:
            line_list = line.strip().split()
            cache[line_list[0].lower()] = (line_list[1], int(line_list[2]))
        return cache

    def write_log(self, msg):
        with open(self.log_dir, 'a', encoding='utf-8') as f:
            f.write(msg)

    def accept(self):
        return self.server_socket.accept()

    def recv_query(self, connection_):
        try:
            data = connection_.recv(self.msg_size)
        except ConnectionResetError:
            connection_.close()
            return ''

        query_ = str(data, encoding='utf-8')

        self.write_log(query_[1:-1] + '\n')
        return query_

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

        '''recursively or iteratively ask next level DNS'''
        top_level_domain = domain.split('.')[-1]
        next_address = self.dns_server_dict.get(top_level_domain, '')
        if next_address == '':
            send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
            return None

        if method == 'R':
            '''Query on behalf of user.'''
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.settimeout(3)
                client_socket.connect(next_address)
                # print(next_address)
                client_socket.sendall(bytes(query, encoding="utf-8"))
                ret_bytes = client_socket.recv(self.msg_size)
                # client_socket.close()
                response_msg = str(ret_bytes, encoding="utf-8")
                self.write_log(response_msg[1:-1] + '\n')

            except ConnectionResetError:
                send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')
                return None

            except ConnectionRefusedError:
                send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')
                return None

            except socket.timeout:
                send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')
                return None

            response_msg_from_next = response_msg
            response_msg_from_next_list = response_msg_from_next[1:-1].split(',')
            code = response_msg_from_next_list[0].strip()
            ip = response_msg_from_next_list[2].strip()

            # if code == '0x00':
            #     ip = response_msg_from_next_list[2].strip()
            #     self.dns_cache[domain] = ip

            send_msg = "<{0}, {1}, {2}>".format(code, self.id, ip)
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')

        elif method == "I":
            '''Return next TLS server address.'''
            send_msg = "<0x01, {0}, {1}, {2}>".format(self.id, next_address[0], next_address[1])
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')


def process_connection(server, connection, address):
    while True:
        query = server.recv_query(connection)
        if query == '':
            print('Loss connection: {0}, {1}'.format(address[0], address[1]))
            break

        server.resolve_query(query, connection, address)
        server.write_log('\n')
        time.sleep(3)
        connection.close()
        break


if __name__ == '__main__':
    root_server = DNSRootServer("Root_DNS_Server", 5353, './data/server.dat')
    print("server start!")

    while True:
        connection, address = root_server.accept()
        print('accept: {0}, {1}'.format(address[0], address[1]))
        connection_thread = threading.Thread(target=process_connection, args=(root_server, connection, address))
        connection_thread.daemon = False
        connection_thread.start()

    root_server.close()
