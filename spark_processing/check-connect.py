import socket

def check_connection(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        if result == 0:
            print(f"Connection to {host}:{port} succeeded")
        else:
            print(f"Connection to {host}:{port} failed")

if __name__ == "__main__":
    check_connection("kafka", 9092)
