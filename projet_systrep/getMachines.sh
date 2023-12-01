#!/bin/bash

usage() {
  echo "Usage: $0 <number of machines>"
  echo "  <number of machines>: the number of machines to select"
  exit 1
}

if [[ $# -ne 1 ]]; then
  usage
fi

login="msauveur-23"

[[ -e index.html ]] && rm index.html

wget tp.telecom-paris.fr
ok_machines=$(cat index.html | grep "</tr>" | sed 's/<\/tr>/\n/g' | grep OK | grep -o tp-[1-9][a-z][0-9]*-[0-9]*  | awk '{print $1".enst.fr"}')

tmp=$(echo $ok_machines | tr "\n" " ")

machines=()

read -ra machines <<< "$tmp"

shuffled_machines=($(shuf -e "${machines[@]}"))

working_count=0

> machines.txt

#this loop will try to connect to each machine in the list and if it succeeds it will add it to the machines.txt file 
#and if the number of machines in the file is equal to the number of machines we want to use it will stop

for machine in "${shuffled_machines[@]}"; do
  echo "Checking machine $machine"
  ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@$machine "exit"
  if [ $? -eq 0 ]; then
    echo "Machine added successfully: $machine"
    echo "$machine" >> machines.txt
    ((working_count++))
  else
    echo "Machine $machine is not working"
  fi
  if [ $working_count -eq $1 ]; then
    break
  fi
done