import os
import sys
import socket
import threading



HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)


def receive_messages(client_sock: socket.socket):
    while True:
        try:
            data = client_sock.recv(1024)
            if not data:
                break
            print(f"\n{data.decode('utf8')}\nEnter message: ", end="")
        except Exception:
            break

def run_client():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, PORT))
            print(f"[CLIENT] Connected to {HOST}:{PORT}")
            
            receive_thread = threading.Thread(target=receive_messages, args=(s,))
            receive_thread.daemon = True # Allows program to exit if main thread closes
            receive_thread.start()

            while True:
                message = input("Enter message: ")
                s.sendall(message.encode('utf8'))
                if message.lower().strip() == '{quit}':
                    break
        except ConnectionRefusedError:
            print("[CLIENT] Could not connect to the server. Make sure the server is running.")
        except Exception as e:
            print(f"[CLIENT] An error occurred: {e}")

if __name__ == "__main__":
    run_client()