import sys
import os


def split_file(file_path, n):
    # Read the .warc.wet file
    with open(file_path, 'rb') as file:
        data = file.read()

    # Get the size of the file
    file_size = len(data)

    # Calculate the split point, which is one nth of the file size
    split_point = file_size // n

    # Ensure the split point is at the end of a line
    while data[split_point] != ord('\n'):
        split_point += 1

    # Split the file into n parts
    parts = []
    for i in range(n):
        if i == n - 1:
            parts.append(data[split_point * i:])
        else:
            parts.append(data[split_point * i:split_point * (i + 1)])

    # Write the parts to new files
    part_paths = []
    for i, part in enumerate(parts):
        part_path = file_path + '.part' + str(i + 1)
        part_paths.append(part_path)
        with open(part_path, 'wb') as file:
            file.write(part)

    return part_paths


n = 0
with open('machines.txt', 'r') as f:
    # replace with the server IP address or hostname
    hosts = [line.strip() for line in f.readlines()]
    n = len(hosts)

splits = split_file(
    "/home/matsvr/projet_systrep/adeployer/splits/travail.txt", n)

print(f"Fichier séparé en {n} parties")

# Supprime le fichier original
os.remove("/home/matsvr/projet_systrep/adeployer/splits/travail.txt")

print("Fichier original supprimé")
