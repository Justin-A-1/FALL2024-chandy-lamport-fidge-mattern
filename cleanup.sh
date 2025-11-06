#!/bin/bash

# Command to grant permission to file to run [RUN THIS]: chmod +x cleanup.sh

# Change this to your netid
netid=jtb200002

# Root directory of project
PROJDIR=/home/013/j/jt/jtb200002/fall24/operating_systems/project1

# Directory where the config file is located on your local system
CONFIGLOCAL=/home/013/j/jt/jtb200002/fall24/operating_systems/project1/config.txt

# Extension for hosts
hostExtension="utdallas.edu"

# Remove comments | remove empty lines | remove carriage returns | select valid lines
validLines=$(cat $CONFIGLOCAL | sed -e 's/#.*//' -e '/^\s*$/d' -e 's/\r$//' | grep -E '^\s*[0-9]')

# Read the valid lines into an array
readarray -t lines <<< "$validLines"

# Check that we have at least one valid line (global parameters)
if [ ${#lines[@]} -lt 1 ]; then
    echo "Error: No valid configuration lines found."
    exit 1
fi

# Read the first valid line and collect the global parameters
firstLine="${lines[0]}"
# Remove any extra whitespace
firstLine=$(echo $firstLine | xargs)
# Extract the six global parameters
globalParams=($firstLine)
if [ ${#globalParams[@]} -lt 6 ]; then
    echo "Error: Global parameters line does not have enough tokens."
    exit 1
fi

numNodes=${globalParams[0]}
echo "Number of Nodes: $numNodes"

# Read node configurations
declare -a hosts

for (( i=0; i<numNodes; i++ ))
do
    lineIndex=$((i + 1)) # Node configurations start from lines[1]
    nodeLine="${lines[$lineIndex]}"
    # Remove any extra whitespace
    nodeLine=$(echo $nodeLine | xargs)
    # Extract nodeID, host, port
    nodeParams=($nodeLine)
    if [ ${#nodeParams[@]} -lt 3 ]; then
        echo "Error: Node configuration line does not have enough tokens."
        exit 1
    fi
    nodeID=${nodeParams[0]}
    host=${nodeParams[1]}
    port=${nodeParams[2]}

    # Add host extension to the string if missing from domain name
    if [[ "$host" != *"$hostExtension"* ]];
    then
        host="$host.$hostExtension"
    fi

    # Store the hosts in an array
    hosts[$i]=$host
done

# Collect unique hosts to avoid sending duplicate commands
uniqueHosts=($(printf "%s\n" "${hosts[@]}" | sort -u))

# Now, for each unique host, issue the kill command
for host in "${uniqueHosts[@]}"
do
    echo "Shutting down all processes on machine $host for netid $netid"

    # Issue command
    echo "Executing command: ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host killall -u $netid"
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host "killall -u $netid" 2>/dev/null &
done

echo "Cleanup complete"
