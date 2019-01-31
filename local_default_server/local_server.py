# encoding = utf-8
# author: Wei Dai
# date: 11/26/2018
"""
# file name: local_server.py
# description:
#   1. This a Python script for query IP address of a domain name, playing a role of DNS default local server.
#   2. The server listen on address (127.0.0.1, 5352). (port: 5352)
#   3. The server receives query in format as <id, domain, method(I/R)>, any other formats will be regarded as invalid.
#   4. When receive domain name with "www." or without "www.", the server will both check whether the two domain names
       are in cache. Because we assume all the domain names with "www." point to the same IP address as its domain name
       without "www.". In fact, www. is a sub-domain, while without www is main domain. E.g. there is no difference
       between [www. bing .com] and [bing .com].
       Reference: https://www.quora.com/What-is-a-webpage-website-without-www-called
#   5.For resolve query, if the domain is not cached, no matter what the method is, it will establish a new connection
      to root DNS server and make a query with the same method.
      5.1. If the method is recursive (R), the result returned by root DNS server is the final answer. Then, send a
           response to client.
      5.2. If the method is iterative (I), root DNS server will send next query address. Then, establish a connection
           and make a query.
#   6. If any of the server that DNS local default server requests break down or loss connection, the local server will
       send <0xFF, {id}, "Host not found"> back to client.
#   7. When receive heartbeat packet from client, the server will give a acknowledgement.
#   8. When manager press ctrl + C (KeyboardInterrupt) or system exit, the server will start to shutdown. It will close
       all the connections and send a broadcast: SERVER_SHUTDOWN: CONNECTION CLOSE to all online users.
#   9. The server will output a log file ({id}.log) whenever it receive or send message to server/client except the
    heartbeat message because heartbeat message is meaningless.
#   10. Every time when the cache update, server will write the cache to default.dat file to ensure next time the server
    can 'remember' history log.

"""


import os
import time
import socket
import threading


class DNSDefaultServer:

    def __init__(self, id_, port_, default_file):
        address_ = ('127.0.0.1', port_)
        
        self.id = id_
        self.default_file = default_file

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(address_)
        self.server_socket.listen(5)

        self.root_address = ('127.0.0.1', 5353)
        self.msg_size = 64 * 1024

        self.dns_cache = self.build_default_cache(default_file)
        
        '''client_connection_list: formatted as [(connection, address), ], i.e. the return of accept'''
        self.client_connection_list = []
        self.client_connection_thread_list = []

        self.server_shutdown = False

        self.log_dir = './log/{0}.log'.format(self.id)

    @staticmethod
    def build_default_cache(file):
        cache = {}
        with open(file) as f:
            data = f.readlines()
        for line in data:
            line_list = line.strip().split()
            cache[line_list[0].lower()] = line_list[1]
        return cache

    def accept(self):
        return self.server_socket.accept()

    def recv_query(self, connection_):
        try:
            data = connection_.recv(self.msg_size)
        except ConnectionResetError:
            connection_.close()
            return ''

        query_ = str(data, encoding='utf-8')
        if query_ != "HEARTBEAT_PACKET_ASK":
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
        result1 = self.dns_cache.get(q1, '')
        result2 = self.dns_cache.get(q2, '')
        if result1 == '':
            return result2
        else:
            return result1

    def set_shutdown(self):
        self.server_shutdown = True

    def write_log(self, msg):
        with open(self.log_dir, 'a', encoding='utf-8') as f:
            f.write(msg)

    def write_cache(self):
        with open(self.default_file, 'w', encoding='utf-8') as f:
            for domain, ip in self.dns_cache.items():
                f.write('{0} {1}\n'.format(domain, ip))

    def resolve_query(self, query, connection, address):
        query_list = query[1:-1].split(',')
        if len(query_list) != 3:
            send_msg = "<0xEE, {0}, {1}>".format(self.id, "Invalid format")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
            return None

        domain = query_list[1].strip()
        method = query_list[2].strip()

        valid_set = {'com', 'gov', 'org'}
        if domain.split('.')[-1].strip() not in valid_set:
            send_msg = "<0xEE, {0}, {1}>".format(self.id, "Invalid format")
            connection.sendto(bytes(send_msg, encoding="utf-8"), address)

            self.write_log(send_msg[1:-1] + '\n')
            return None

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
            '''recursively or iteratively ask root DNS'''
            try:
                client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_socket.settimeout(4)
                client_socket.connect(self.root_address)
                send_msg = "<{0}, {1}, {2}>".format(self.id, domain, method)
                client_socket.sendall(bytes(send_msg, encoding="utf-8"))

                self.write_log(send_msg[1:-1] + '\n')

                ret_bytes = client_socket.recv(self.msg_size)
                response_msg = str(ret_bytes, encoding="utf-8")

                self.write_log(response_msg[1:-1] + '\n')

            except ConnectionResetError:
                send_msg = "<0xFF, {0}, {1}>".format(self.id, "Host not found")
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)
                print('ConnectionResetError')

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
                print('timeout')

                self.write_log(send_msg[1:-1] + '\n')
                return None

            if method == 'R':
                '''Recursive query, the result is the final answer.'''
                response_msg_from_root = response_msg
                response_msg_from_root_list = response_msg_from_root[1:-1].split(',')
                code = response_msg_from_root_list[0].strip()

                if code == '0x00':
                    ip = response_msg_from_root_list[2].strip()
                    self.dns_cache[domain] = ip
                    self.write_cache()

                send_msg = "<{0}, {1}, {2}>".format(code, self.id, response_msg_from_root_list[2].strip())
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')
                
            elif method == "I":
                '''Iterative query, the result is the next query address'''
                response_msg_list = response_msg[1:-1].split(',')
                code = response_msg_list[0].strip()

                while code != '0x00' and code != '0xFF':
                    next_address = (response_msg_list[2].strip(), int(response_msg_list[3].strip()))

                    try:
                        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client_socket.settimeout(4)
                        client_socket.connect(next_address)
                        send_msg = "<{0}, {1}, {2}>".format(self.id, domain, method)
                        client_socket.sendall(bytes(send_msg, encoding="utf-8"))

                        self.write_log(send_msg[1:-1] + '\n')

                        '''Wait for response'''
                        response_msg = str(client_socket.recv(self.msg_size), encoding="utf-8")

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

                    response_msg_list = response_msg[1:-1].split(',')
                    code = response_msg_list[0].strip()

                '''When jump from while loop, the result must be the final response.'''
                if code == '0x00':
                    ip = response_msg_list[2].strip()
                    self.dns_cache[domain] = ip
                    self.write_cache()

                send_msg = "<{0}, {1}, {2}>".format(code, self.id, response_msg_list[2].strip())
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')

            else:
                send_msg = "<0xEE, {0}, {1}>".format(self.id, "Invalid format")
                connection.sendto(bytes(send_msg, encoding="utf-8"), address)

                self.write_log(send_msg[1:-1] + '\n')
                return None


def process_connection(server):
    connection, address = server.accept()
    server.client_connection_list.append((connection, address))
    print('accept: {0}, {1}'.format(address[0], address[1]))
    while True:
        if server.server_shutdown:
            '''Sever shutdown because of interruption. It will broadcast a message and close all connection'''
            connection.sendto(bytes("SERVER_SHUTDOWN: CONNECTION CLOSE", encoding="utf-8"), address)
            server.write_log("SERVER_SHUTDOWN: CONNECTION CLOSE: {0}. {1}".format(address[0], address[1]) + '\n\n')

            time.sleep(5)
            connection.close()
            break
        else:
            query = server.recv_query(connection)
            if query == '':
                print('Loss connection: {0}, {1}'.format(address[0], address[1]))
                break
            if query == 'q':
                print('close: {0}, {1}'.format(address[0], address[1]))
                connection.close()
                break
            if query == "HEARTBEAT_PACKET_ASK":
                ''' This is for heartbeat protocol, which follows the traditional TCP.'''
                connection.sendto(bytes("HEARTBEAT_PACKET_ACK", encoding="utf-8"), address)
                pass

            else:
                server.resolve_query(query, connection, address)
                server.write_log('\n')


server = DNSDefaultServer("Local_DNS_Server", 5352, './data/default.dat')
print("server start!")

while True:
    try:
        '''This design is to avoid to generate thread infinitely.'''
        if len(server.client_connection_thread_list) == len(server.client_connection_list):
            connection_thread = threading.Thread(target=process_connection, args=(server, ))
            connection_thread.daemon = False
            connection_thread.start()

            server.client_connection_thread_list.append(connection_thread)

    except SystemExit:
        server.set_shutdown()
        time.sleep(5)
        os._exit(1)

    except KeyboardInterrupt:
        print("Shutting down sever. Sever will close in 10 seconds.")
        server.set_shutdown()
        time.sleep(10)
        os._exit(1)


server.close()
