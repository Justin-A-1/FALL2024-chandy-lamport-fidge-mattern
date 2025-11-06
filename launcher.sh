#!/bin/bash

# Change this to your netid
netid=jtb200002

# Root directory of project
PROJDIR=/home/013/j/jt/jtb200002/fall24/operating_systems/project1

# Directory where the config file is located on your local system
CONFIGLOCAL=/home/013/j/jt/jtb200002/fall24/operating_systems/project1/config.txt

# Directory your compiled classes are in
BINDIR=$PROJDIR

# Your main project class
PROG=Node

# Extension for hosts
hostExtension="utdallas.edu"

# Initialize arrays to store node configurations and neighbor lists
declare -a nodeIDs
declare -a hosts
declare -a ports
declare -a neighborLists

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
minPerActive=${globalParams[1]}
maxPerActive=${globalParams[2]}
minSendDelay=${globalParams[3]}
snapshotDelay=${globalParams[4]}
maxNumber=${globalParams[5]}

echo "Global Parameters:"
echo "Number of Nodes: $numNodes"
echo "minPerActive: $minPerActive"
echo "maxPerActive: $maxPerActive"
echo "minSendDelay: $minSendDelay"
echo "snapshotDelay: $snapshotDelay"
echo "maxNumber: $maxNumber"

# Now, we need to read the next n lines for node configurations
# Check that we have enough lines
totalValidLines=${#lines[@]}
expectedTotalLines=$((2 * numNodes + 1))

if [ $totalValidLines -lt $expectedTotalLines ]; then
    echo "Error: Not enough valid lines in configuration file."
    exit 1
fi

# Read node configurations
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

    # Store the node configurations in arrays
    nodeIDs[$i]=$nodeID
    hosts[$i]=$host
    ports[$i]=$port
done

# Read neighbor lists
for (( i=0; i<numNodes; i++ ))
do
    lineIndex=$((numNodes + 1 + i)) # Neighbor lists start from lines[numNodes + 1]
    neighborLine="${lines[$lineIndex]}"
    # Remove any extra whitespace
    neighborLine=$(echo $neighborLine | xargs)
    # Extract neighbor IDs (could be zero or more)
    neighborLists[$i]="$neighborLine"
done

# Now, launch the nodes
for (( i=0; i<numNodes; i++ ))
do
    nodeID=${nodeIDs[$i]}
    host=${hosts[$i]}
    port=${ports[$i]}
    neighbors=${neighborLists[$i]}

    echo "Launching Node $nodeID on $host with port $port"

    # Construct the run command with the required arguments, including the config file path
    runCommand="java -cp $BINDIR $PROG $numNodes $minPerActive $maxPerActive $minSendDelay $snapshotDelay $maxNumber $nodeID $host $port \"$neighbors\" $CONFIGLOCAL"

    # Issue command to launch the node on the correct host machine
    echo issued command: gnome-terminal -- bash -c "ssh -t -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host '$runCommand; exec bash'" &
    gnome-terminal -- bash -c "ssh -t -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host '$runCommand; exec bash'" &
    #sleep 1
done

echo "Launcher complete"
