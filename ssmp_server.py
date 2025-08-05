import socket
import struct
import json
import threading

# A thread-safe in-memory key-value store
class KeyValueStore:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def delete(self, key):
        with self._lock:
            if key in self._data:
                del self._data[key]
                return 1  # Number of keys deleted
            return 0

    def exists(self, key):
        with self._lock:
            return 1 if key in self._data else 0

# Global instance of our data store
DATA_STORE = KeyValueStore()

def pack_message(data):
    """Packs a Python list/dict into an SSMP message (bytes)."""
    payload = json.dumps(data).encode('utf-8')
    # Prepend the payload length as a 4-byte, big-endian unsigned integer
    header = struct.pack('!I', len(payload))
    return header + payload

def read_fully(sock, length):
    """Reads exactly 'length' bytes from a socket."""
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

def handle_client(connection, address):
    """Handles a single client connection."""
    print(f"[INFO] Accepted connection from {address}")
    try:
        while True:
            # 1. Read the 4-byte header to get the payload length
            header = read_fully(connection, 4)
            if not header:
                break  # Client closed connection
            
            payload_length = struct.unpack('!I', header)[0]

            # 2. Read the payload of the specified length
            payload = read_fully(connection, payload_length)
            if not payload:
                break

            # 3. Decode and parse the JSON payload
            try:
                command_list = json.loads(payload.decode('utf-8'))
                if not isinstance(command_list, list) or not command_list:
                    raise ValueError("Invalid command format")
            except (json.JSONDecodeError, ValueError) as e:
                print(f"[ERROR] Invalid payload from {address}: {e}")
                response = ['ERR', 'INVALID_PAYLOAD']
                connection.sendall(pack_message(response))
                continue

            command = command_list[0].upper()
            args = command_list[1:]
            print(f"[INFO] Received from {address}: {command} {args}")

            # 4. Process the command
            response = None
            if command == 'PING':
                response = ['OK', 'PONG']
            elif command == 'SET' and len(args) == 2:
                DATA_STORE.set(args[0], args[1])
                response = ['OK']
            elif command == 'GET' and len(args) == 1:
                value = DATA_STORE.get(args[0])
                if value is not None:
                    response = ['OK', value]
                else:
                    response = ['ERR', 'NOT_FOUND']
            elif command == 'DEL' and len(args) == 1:
                num_deleted = DATA_STORE.delete(args[0])
                response = ['OK', num_deleted]
            elif command == 'EXISTS' and len(args) == 1:
                num_exists = DATA_STORE.exists(args[0])
                response = ['OK', num_exists]
            else:
                response = ['ERR', 'UNKNOWN_COMMAND']
            
            # 5. Send the response back to the client
            connection.sendall(pack_message(response))

    except (ConnectionResetError, BrokenPipeError):
        print(f"[INFO] Client {address} disconnected unexpectedly.")
    finally:
        print(f"[INFO] Closing connection to {address}")
        connection.close()

def main(host='127.0.0.1', port=6380):
    """Main function to start the server."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Reuse address to avoid "Address already in use" errors
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"[INFO] SSMP Server listening on {host}:{port}")

    try:
        while True:
            connection, address = server_socket.accept()
            # Create a new thread for each client
            thread = threading.Thread(target=handle_client, args=(connection, address))
            thread.daemon = True # Allows main thread to exit even if client threads are running
            thread.start()
    except KeyboardInterrupt:
        print("\n[INFO] Server is shutting down.")
    finally:
        server_socket.close()

if __name__ == '__main__':
    main()
