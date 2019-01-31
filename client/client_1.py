# encoding = utf-8
# author: Wei Dai
# date: 11/26/2018
"""
# file name: client_1.py
# description:
#   1. This a Python script for query IP address of a domain name, playing a role of DNS client.
#   2. User input must be domain_name method (I/R) split by any blank character i.e. \t or space.
#   3. When user input q, the connection between client and server will be closed, and client will close.
#   4. There is a thread tp handle all received message from server and a heartbeat detection. If the server crash down
       when the client still running, client will receive a broadcast: SERVER_SHUTDOWN: CONNECTION CLOSE.
#   5. The client will output a log file ({id}.log) whenever it receive or send message to server except the heartbeat
       message because heartbeat message is meaningless.
"""


import os
import time
import socket
import threading


class DNSClient:

    def __init__(self, id_, ip_, port_):
        self.id = id_

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((ip_, port_))
        self.client_socket.settimeout(5)

        self.msg_sent = False
        self.close_sent = False
        self.server_online = True

        self.log_dir = './log/{0}.log'.format(self.id)

    def send_query(self, domain, method):
        """query format: <id, hostname, I/R>"""

        query = '<{0}, {1}, {2}>'.format(self.id, domain, method)
        self.client_socket.sendall(bytes(query, encoding="utf-8"))
        self.write_log(query[1:-1] + '\n')

        self.msg_sent = True
        return None

    def recv_msg(self):
        recv_bytes = self.client_socket.recv(1024)
        recv_str = str(recv_bytes, encoding="utf-8")
        return recv_str

    def send_close(self):
        self.client_socket.sendall(bytes('q', encoding="utf-8"))
        self.write_log('q\n')

        self.close_sent = True
        return None

    def close(self):
        self.client_socket.close()

    def set_msg_sent_false(self):
        self.msg_sent = False

    def set_server_offline(self):
        self.server_online = False

    def send_heartbeat(self):
        self.client_socket.sendall(bytes("HEARTBEAT_PACKET_ASK", encoding="utf-8"))

    def write_log(self, msg):
        with open(self.log_dir, 'a', encoding='utf-8') as f:
            f.write(msg)


def recv_and_heartbeat_thread(client):
    """ This thread function handles all received message from sever and maintain a heartbeat process"""
    timer_start = time.time()
    n_time_out = 0
    while True:
        ''' If main process is end, thread should be end.'''
        if client.close_sent:
            return None

        ''' This part is for heartbeat signal.
            Every 3 seconds send a signal to ensure the server is not down.
            If received msg is SERVER_SHUTDOWN: CONNECTION CLOSE instead of HEARTBEAT_PACKET_ACK, it means the server is
            down and the thread and process should be ended.
        '''
        if n_time_out > 50:
            print('Time out, connection close.')
            client.close()
            os._exit(1)

        if time.time() - timer_start > 3.0:
            try:
                client.send_heartbeat()
                timer_start = time.time()
                msg = client.recv_msg()
                if msg == 'SERVER_SHUTDOWN: CONNECTION CLOSE':
                    client.write_log(msg + '\n')
                    print("\n" + msg)

                    client.set_server_offline()

                    print("\nServer Down. Client will end in 3 second.")

                    time.sleep(3)
                    os._exit(-1)
                else:
                    n_time_out = 0

            except ConnectionResetError:
                print("\nServer Down. Client will end in 3 second.")
                time.sleep(3)
                os._exit(-3)

                return None
            except socket.timeout:
                n_time_out += 1
                continue

        ''' This part is for receive query result'''
        if client.msg_sent:
            try:
                msg = client.recv_msg()
                client.write_log(msg[1:-1] + '\n\n')
                print(msg)

                client.set_msg_sent_false()
            except socket.timeout:
                print('Time out, please try again!')
                client.set_msg_sent_false()
                continue


if __name__ == "__main__":
    try:
        client = DNSClient("PC1", '127.0.0.1', 5352)
    except ConnectionRefusedError:
        print("Cannot connect to server. Client will close.")
        os._exit(1)
    else:
        connection_thread = threading.Thread(target=recv_and_heartbeat_thread, args=(client,))
        connection_thread.daemon = False
        connection_thread.start()

        while True:
            if not client.msg_sent:
                query = input('query: ')
                if client.server_online:
                    if query == 'q':
                        client.send_close()
                        print('Client Closing: The client will close in 3 seconds.')
                        time.sleep(3)
                        client.close()
                        break
                    else:
                        query_list = query.split()
                        if len(query_list) == 2:
                            domain = query.split()[0].lower()
                            method = query.split()[1].upper()
                            client.send_query(domain, method)
                        else:
                            print('Invalid Input. Please try again.')
                else:
                    print('Server Offline. Client will be closed.')
                    break
