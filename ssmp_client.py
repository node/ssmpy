import socket
import struct
import json

class SSMPClient:
    def __init__(self, host='127.0.0.1', port=6380):
        self.host = host
        self.port = port
        self._socket = None

    def connect(self):
        """Establishes a connection to the server."""
        if self._socket:
            self.close()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.host, self.port))

    def close(self):
        """Closes the connection."""
        if self._socket:
            self._socket.close()
            self._socket = None

    def _pack_message(self, data):
        """Packs a Python list/dict into an SSMP message (bytes)."""
        payload = json.dumps(data).encode('utf-8')
        header = struct.pack('!I', len(payload))
        return header + payload
    
    def _read_fully(self, length):
        """Reads exactly 'length' bytes from the socket."""
        data = bytearray()
        while len(data) < length:
            packet = self._socket.recv(length - len(data))
            if not packet:
                raise ConnectionError("Socket connection broken")
            data.extend(packet)
        return data

    def _send_request(self, command_list):
        """Sends a request and waits for a single response."""
        if not self._socket:
            raise ConnectionError("Client not connected. Call connect() first.")
        
        # 1. Pack and send the request
        message = self._pack_message(command_list)
        self._socket.sendall(message)

        # 2. Read the 4-byte header for the response
        header = self._read_fully(4)
        payload_length = struct.unpack('!I', header)[0]

        # 3. Read the response payload
        payload = self._read_fully(payload_length)
        
        # 4. Decode and return the response
        response = json.loads(payload.decode('utf-8'))
        return response

    # --- Public API methods ---
    
    def ping(self):
        return self._send_request(['PING'])

    def get(self, key):
        return self._send_request(['GET', key])

    def set(self, key, value):
        return self._send_request(['SET', key, value])

    def delete(self, key):
        return self._send_request(['DEL', key])

    def exists(self, key):
        return self._send_request(['EXISTS', key])
    
    # --- Context Manager Support ---

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def main():
    """Demonstration of the SSMPClient."""
    print("--- Connecting to SSMP Server ---")
    try:
        with SSMPClient('127.0.0.1', 6380) as client:
            print("Client connected successfully.")

            # 1. PING
            print("\n1. Pinging server...")
            response = client.ping()
            print(f"   -> Server response: {response}")
            assert response == ['OK', 'PONG']

            # 2. SET a value
            print("\n2. Setting key 'user:1' to 'Alice'...")
            response = client.set('user:1', 'Alice')
            print(f"   -> Server response: {response}")
            assert response == ['OK']

            # 3. GET the value back
            print("\n3. Getting key 'user:1'...")
            response = client.get('user:1')
            print(f"   -> Server response: {response}")
            assert response == ['OK', 'Alice']

            # 4. Check if a key EXISTS
            print("\n4. Checking if 'user:1' exists...")
            response = client.exists('user:1')
            print(f"   -> Server response: {response}")
            assert response == ['OK', 1]

            # 5. DELETE the key
            print("\n5. Deleting key 'user:1'...")
            response = client.delete('user:1')
            print(f"   -> Server response: {response}")
            assert response == ['OK', 1]

            # 6. Check if the key still EXISTS
            print("\n6. Checking if 'user:1' exists again...")
            response = client.exists('user:1')
            print(f"   -> Server response: {response}")
            assert response == ['OK', 0]

            # 7. Try to GET a non-existent key
            print("\n7. Trying to get deleted key 'user:1'...")
            response = client.get('user:1')
            print(f"   -> Server response: {response}")
            assert response == ['ERR', 'NOT_FOUND']
            
            print("\n--- All tests passed! ---")

    except ConnectionRefusedError:
        print("\n[ERROR] Connection refused. Is the server running?")
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()
