import socket
import threading
import struct
import traceback
import time
import hashlib
import json

map_results = []
shuffle_results = []
reduce_results = {}
time_storage = {}


def consistent_hash(word):
    return int(hashlib.md5(word.encode()).hexdigest(), 16)


def send_one_message(sock, data):
    length = len(data)
    sock.sendall(struct.pack('!I', length))
    sock.sendall(data)


def recv_one_message(sock):
    lengthbuf = recvall(sock, 4)
    if not lengthbuf:
        return ''
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)


def recvall(sock, count):
    fragments = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            return None
        fragments.append(chunk)
        count -= len(chunk)
    arr = b''.join(fragments)
    return arr


def handle_client(client_socket, address):
    print(f'{socket.gethostname()} New client connected: {address}')
    try:
        while True:
            data = recv_one_message(client_socket)
            if not data:
                break
            message = data.decode().strip().lower()  # convert message to lowercase
            print(
                f'{socket.gethostname()} Received message from {address}: {message}')

            if message == '------> hello !! <------':
                response = 'ok !!'
                send_one_message(client_socket, response.encode())
                print("\n")

            elif message == 'file':
                # receive filename
                filename = recv_one_message(client_socket).decode().strip()
                print(
                    f'{socket.gethostname()} Received filename from {address}: {filename}')

                # receive filesize
                filesize = int(recv_one_message(
                    client_socket).decode().strip())
                print(
                    f'{socket.gethostname()} Received filesize from {address}: {filesize}')

                # receive file data
                with open(filename, 'wb') as f:
                    remaining_bytes = filesize
                    while remaining_bytes > 0:
                        data = recv_one_message(client_socket)
                        if not data:
                            break
                        f.write(data)
                        remaining_bytes -= len(data)
                print(
                    f'{socket.gethostname()} Received the entire file from {address}: {filesize}')
                response = 'File received'
                send_one_message(client_socket, response.encode())
                print("\n")

            # Si le message est go, le host envoie hello aux autres hosts
            elif message == '------> go <------':
                # Open the hosts file and read the hosts into a list
                with open('machines.txt', 'r') as f:
                    # replace with the server IP address or hostname
                    hosts = [line.strip() for line in f.readlines()]
                port = 6900
                i = 0
                for host1 in hosts:
                    if socket.gethostbyname(host1) != socket.gethostbyname(socket.gethostname()):
                        client_socket1 = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)
                        client_socket1.connect((host1, port))
                        # send "Hello t es ready ?"
                        message = '------> hello tout est bon ? <------'
                        send_one_message(client_socket1, message.encode())
                        time.sleep(1)

                        response = recv_one_message(client_socket1).decode()
                        # send_one_message(client_socket1, response.encode())
                        time.sleep(1)
                        print(f'Received response from {host1}: {response}')
                        i += 1

                    # Dès qu'un host a demandé "tes ready" à toutes les autres machines, on arrete le processus
                    if i == len(hosts):
                        break
                response = '------> Tout le monde a repondu, lets go ! <------'
                send_one_message(client_socket, response.encode())

            elif message == '------> hello tout est bon ? <------':
                response = 'oui !!'
                send_one_message(client_socket, response.encode())
                time.sleep(1)

            # Opération 'map'
            elif message == '------> map start <------':
                # Chronométrage du mapping
                total_map_time = 0
                start_time_map = time.time()
                print(
                    f"Map operarting on {socket.gethostbyname(socket.gethostname())}")
                # Reception du fichier a mapper
                split = recv_one_message(client_socket).decode().strip()
                print(
                    f'The split file received by {socket.gethostname()} is: {split}')
                with open(split, 'r', encoding='utf-8') as file:
                    for line in file:
                        words = line.split()
                        for word in words:
                            word = word.strip('.,!?":;()[]{}')
                            word = word.lower()
                            map_results.append(word)
                # FIn du chronoétrage du mapping
                end_time_map = time.time()
                map_time = end_time_map - start_time_map
                total_map_time = total_map_time + map_time
                time_storage['map_time'] = total_map_time
                print(f"Map results: {map_results}")
                # print(f"Map operation is done in {total_map_time} seconds\n")

                # Mapping fini
                message = f"map operation is done in {total_map_time} seconds"
                send_one_message(client_socket, message.encode())

            # Opération 'shuffle'
            elif message == '------> shuffle start <------':
                # Chronométrage du shuffle
                total_shuffle_time = 0
                start_time_shuffle = time.time()
                print(
                    f"Shuffle operarting on {socket.gethostbyname(socket.gethostname())}")
                for word in map_results:
                    # On définit la machine qui va traiter le mot en utilisant une fonction de hashage sur ce mot
                    with open("machines.txt", 'r', encoding='utf-8') as f:
                        hosts = [line.strip() for line in f.readlines()]
                        selected_host = hosts[consistent_hash(
                            word) % len(hosts)]
                        print(
                            f"Selected host for the word '{word}': {selected_host}")
                    # On envoie le mot à la machine sélectionnée
                    try:
                        port = 6900
                        server = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)
                        server.connect((selected_host, port))

                        message = 'operating shuffle'
                        send_one_message(server, message.encode())
                        send_one_message(server, word.encode())
                        print(
                            f"Word '{word}' has been sent to {selected_host}")
                        response = recv_one_message(server).decode()
                        print(response)

                        # Fermeture de la connexion avec la machine sélectionnée
                        server.close()

                    except Exception as e:
                        print(f'Unexpected error : {e}')
                        traceback.print_exc()

                # Fin du chronoétrage du shuffle
                end_time_shuffle = time.time()
                shuffle_time = end_time_shuffle - start_time_shuffle
                total_shuffle_time += shuffle_time
                time_storage['shuffle_time'] = total_shuffle_time
                print(
                    f"Shuffle operation is done in {total_shuffle_time} seconds\n")

                message = f"shuffle operation is done in {total_shuffle_time} seconds"
                send_one_message(client_socket, message.encode())
                # print(message, "\n")

            elif message == 'operating shuffle':
                word = recv_one_message(client_socket).decode().strip()
                print(f"Received word '{word}' from {address}")
                shuffle_results.append(word)
                response = f"Word received by {socket.gethostname()}"
                send_one_message(client_socket, response.encode())
                print("\n")

            # Opération 'reduce'
            elif message == '------> reduce start <------':
                # Chronométrage du reduce
                total_reduce_time = 0
                start_time_reduce = time.time()
                # reduce_results = {}
                for word in shuffle_results:
                    if word in reduce_results:
                        reduce_results[word] += 1
                    else:
                        reduce_results[word] = 1

                print(f"Reduce results: {reduce_results}")

                # Fin du chronoétrage du reduce
                end_time_reduce = time.time()
                reduce_time = end_time_reduce - start_time_reduce
                total_reduce_time += reduce_time
                time_storage['reduce_time'] = total_reduce_time
                print(
                    f"Reduce operation is done in {total_reduce_time} seconds\n")

                message = f"reduce operation is done in {total_reduce_time} seconds"
                send_one_message(client_socket, message.encode())
                # print(message, "\n")

            # On envoie les résultats de reduce au client
            elif message == '------> envoie moi les résultats <------':
                # total_time_overall = 0
                # total_time_overall = total_map_time + total_shuffle_time + total_reduce_time
                # else:
                #     print("Erreur : l'une des variables de temps n'est pas initialisée")
                message1 = time_storage
                message1_json = json.dumps(message1)
                message2 = reduce_results
                message2_json = json.dumps(message2)
                send_one_message(client_socket, message1_json.encode())
                send_one_message(client_socket, message2_json.encode())
                print("\n")

            # Fin
            elif message == '------> bye !! <------':
                print("\n")
                break

    except Exception as e:
        print(f'{socket.gethostname()} Error handling client {address}: {e}')
        traceback.print_exc()
    finally:
        client_socket.close()
        print(f'{socket.gethostname()} Client disconnected: {address}')


def start_server(port):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen()
        print(f'{socket.gethostname()} Server listening on port {port}')

        while True:
            client_socket, address = server_socket.accept()
            print(f'{socket.gethostname()} Accepted new connection from {address}')
            client_thread = threading.Thread(
                target=handle_client, args=(client_socket, address))
            client_thread.start()
    except Exception as e:
        print(f'{socket.gethostname()} Error starting server: {e}')
        traceback.print_exc()
    finally:
        server_socket.close()


if __name__ == '__main__':
    port = 6900  # pick any free port you wish that is not used by other students
    start_server(port)
