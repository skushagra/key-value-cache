import socket
import struct
import time

target_host = "0.0.0.0"
target_port = 7171

def serialize_command(command):
    """Convert user input into binary format."""
    payload = command.split(" ")
    cmd_type = payload[0]

    try:
        if cmd_type == "get":
            key = payload[1].encode()
            return struct.pack(f"!BH{len(key)}s", 1, len(key), key)

        elif cmd_type == "set":
            key = payload[1].encode()
            value = payload[2].encode()
            print(struct.pack(f"!BH{len(key)}sH{len(value)}s", 2, len(key), key, len(value), value))
            return struct.pack(f"!BH{len(key)}sH{len(value)}s", 2, len(key), key, len(value), value)

        elif cmd_type == "delete":
            key = payload[1].encode()
            return struct.pack(f"!BH{len(key)}s", 3, len(key), key)

        else:
            raise Exception("Invalid command. Use: get <key>, set <key> <value>, delete <key>")

    except IndexError:
        raise Exception("Missing arguments. Use: get <key>, set <key> <value>, delete <key>")

def deserialize_response(response):
    """Decode binary response."""
    message_len = struct.unpack("!H", response[:2])[0]
    return response[2:2+message_len].decode()

def client(command):
    """Client function to send and receive messages."""
    command_message = serialize_command(command)
    
    channel = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    channel.connect((target_host, target_port))
    
    channel.send(command_message)
    start_time = time.perf_counter()

    channel.settimeout(0.1)  # Wait max 100ms for response
    try:
        response = channel.recv(512)  # Blocking recv with timeout
    except socket.timeout:
        response = b"\x00\x00Error: Timeout"

    end_time = time.perf_counter()
    channel.close()

    print(f"Response: {deserialize_response(response)} | Time: {(end_time - start_time) * 1000:.3f} ms")

if __name__ == "__main__":
    print("Welcome to a random cache!")
    index = 0
    while True:
        try:
            command = input(f"cache[{index}] ")

            if command == "exit":
                print("Bye!")
                break
            elif command == "clear":
                print("\033[H\033[J", end="")
                index = 0
                continue
            elif command == "":
                print("\033[H\033[J", end="")
            elif command == "help":
                print("Available commands: get <key>, set <key> <value>, delete <key>, exit, clear, help")
            else:
                client(command)

        except Exception as e:
            print("Error:", e)

        index += 1
