import socket

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET,
                      socket.SOCK_STREAM)  # Socket will create with TCP and IP protocols
    s.bind(('localhost', 9999))  # This method will bind the sockets with server and port no
    s.listen(1)  # Will allow a maximum of one connection to the socket
    print("listening")
    c, addr = s.accept()  # will wait for the client to accept the connection

    print("CONNECTION FROM:", str(addr))  # Will display the address of the client

    c.send(b"HELLO, Are you enjoying programming?/Great! Keep going")  # Will send message to the client after encoding

    msg = "Take Care.............."
    c.send(msg.encode())
    c.close()  # Will disc`
