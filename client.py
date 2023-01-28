import socket

if __name__ == "__main__":
    import socket

    s = socket.socket(socket.AF_INET,
                      socket.SOCK_STREAM)  # Socket will create with TCP and, IP protocols
    s.connect(('localhost', 9999))  # Will connect with the server
    msg = s.recv(1024)  # Will receive the reply message string from the server at 1024 B

    while msg:
        print('Received:' + msg.decode())
        msg = s.recv(1024)  # Will run as long as the message string is empty

    s.close()