import socket
from time import time
import threading


MEMORY = {}


def milliseconds():
    return int(time() * 1000)


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    def thread_function(sock):
        while True:
            data: bytes = sock.recv(1024)
            data_as_str = data.decode()

            # TODO: need to parse command out instead
            arr_length = int(data_as_str[1])

            if arr_length == 2:
                if 'echo' in data_as_str:
                    # echo command
                    bulk_str = data_as_str[data_as_str.index('echo')+6:]
                    sock.send(bulk_str.encode())
                else:
                    as_arr = data_as_str.split('\r\n')
                    key = as_arr[-2]
                    print('key ', key)
                    if key in MEMORY:
                        val = MEMORY[key][0]
                        expiry = MEMORY[key][1]
                        if expiry is None:
                            sock.send('${}\r\n{}\r\n'.format(str(len(val)), val).encode())
                        else:
                            if milliseconds() <= expiry:
                                sock.send('${}\r\n{}\r\n'.format(str(len(val)), val).encode())
                            else:
                                sock.send('$-1\r\n'.encode())
                    else:
                        sock.send('$-1\r\n'.encode())
            elif arr_length > 2:
                # set command
                as_arr = data_as_str.split('\r\n')
                if arr_length == 3:  # no extra args
                    key = as_arr[-4]
                    val = as_arr[-2]
                    print(f'setting {key} to {val}')
                    MEMORY[key] = (val, None)
                else:  # px flag
                    key = as_arr[4]
                    val = as_arr[6]
                    expiry = int(as_arr[-2])
                    print(f'setting {key} to {val} with expiry {str(expiry)}')
                    current_time = milliseconds()
                    MEMORY[key] = (val, current_time + expiry)
                sock.send('+OK\r\n'.encode())
            else:
                sock.send('+PONG\r\n'.encode())

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        sock, addr = server_socket.accept()  # wait for client
        thread = threading.Thread(target=thread_function, args=(sock,))
        thread.start()


if __name__ == "__main__":
    main()
