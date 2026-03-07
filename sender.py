import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", 1501))

manager_addr = ("127.0.0.1", 1500)
sock.sendto("hello manager".encode(), manager_addr)

response, addr = sock.recvfrom(1024)
print(f"Manager said: {response.decode()}")