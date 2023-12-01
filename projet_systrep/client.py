import socket
import os
import struct
import traceback
import time
import json


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


def main():
    # Open the hosts file and read the hosts into a list
    with open('machines.txt', 'r') as f:
        # replace with the server IP address or hostname
        hosts = [line.strip() for line in f.readlines()]
        splits = ["/tmp/msauveur-23/adeployer/splits/dompub0.txt",
                  "/tmp/msauveur-23/adeployer/splits/dompub1.txt", "/tmp/msauveur-23/adeployer/splits/dompub2.txt"]
        port = 6900  # replace with the server port number

        for host in hosts:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host, port))

            # send "Hello"
            message = '------> hello !! <------'
            send_one_message(client_socket, message.encode())
            response = recv_one_message(client_socket).decode()
            print(f'Received response from {socket.gethostname()}: {response}')

            # send file
            filename = 'machines.txt'
            filesize = os.path.getsize(filename)
            message = 'file'
            send_one_message(client_socket, message.encode())
            send_one_message(client_socket, filename.encode())
            send_one_message(client_socket, str(filesize).encode())
            with open(filename, 'rb') as f:
                data = f.read(1024)
                while data:
                    send_one_message(client_socket, data)
                    # add a 1 second delay between sending messages
                    time.sleep(1)
                    data = f.read(1024)
            response = recv_one_message(client_socket).decode()
            print(f'Received response by {socket.gethostname()}: {response}')

        print("\n")

        # Nous allons faire les machines se dire bonjour entre elles
        for host1 in hosts:
            client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host1, port))

            # Envoie Go a host
            message = '------> go <------'
            send_one_message(client_socket, message.encode())
            response = recv_one_message(client_socket).decode()
            print(f'Received response from {host1}: {response}')
        time.sleep(2)

        print("\n")

        # Fais débuter la phase de map sur tous les hosts en leur affectant un fichier à lire parmi S0, S1 et S2
        for index, host1 in enumerate(hosts):
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((host1, port))
                # Envoie MAP_START à host
                message = '------> map start <------'
                send_one_message(client_socket, message.encode())
                send_one_message(client_socket, splits[index].encode())
                response = recv_one_message(client_socket).decode()

            except Exception as e:
                print(f"Cannot connect with {host1}: {e}")
                traceback.print_exc()

        print("map operation finished !")
        time.sleep(2)

        # Fais débuter la phase de shuffle sur tous les hosts
        for host1 in hosts:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((host1, port))
                # Envoie SHUFFLE_START à host
                message = '------> shuffle start <------'
                send_one_message(client_socket, message.encode())
                response = recv_one_message(client_socket).decode()

            except Exception as e:
                print(f"Cannot connect with {host1}: {e}")
                traceback.print_exc()

        print("shuffle operation finished !")
        time.sleep(2)

        # Fais débuter la phase de reduce sur tous les hosts
        for host1 in hosts:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((host1, port))
                # Envoie REDUCE_START à host
                message = '------> reduce start <------'
                send_one_message(client_socket, message.encode())
                response = recv_one_message(client_socket).decode()

            except Exception as e:
                print(f"Cannot connect with {host1}: {e}")
                traceback.print_exc()

        print("reduce operation finished !")
        time.sleep(2)

        print("\n")

        # Combiner les résultats du reduce de chaque host et récupérer le temps d'exécution
        reslutats = {}
        operations_durations = {}
        for host1 in hosts:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((host1, port))
                message = '------> Envoie moi les résultats <------'
                send_one_message(client_socket, message.encode())
                response1_json = recv_one_message(client_socket).decode()
                response1 = json.loads(response1_json)
                response2_json = recv_one_message(client_socket).decode()
                response2 = json.loads(response2_json)
                operations_durations[host1] = response1
                reslutats[host1] = response2

            except Exception as e:
                print(f"Cannot connect with {host1}: {e}")
                traceback.print_exc()

        # On print dans le terminal le temps d'exécution de chaque opération et les résultats pour chaque host
        print("Operations durations:")
        for host1 in hosts:
            print(f"{host1}: {operations_durations[host1]}\n")
        print("Resultats: ")
        for host1 in hosts:
            print(f"{host1}: {reslutats[host1]}\n")

        # On finit le programme en disant bye à chaque host
        for host1 in hosts:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                client_socket.connect((host1, port))
                message = '------> Bye !! <------'
                send_one_message(client_socket, message.encode())
                client_socket.close()

            except Exception as e:
                print(f"Cannot connect with {host1}: {e}")
                traceback.print_exc()


if __name__ == '__main__':
    main()
