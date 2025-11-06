import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

/**
 * This class implements a node in a distributed system that uses the Chandy-Lamport snapshot algorithm
 * for consistent global snapshots and vector clocks for message ordering.
 */
public class Node {

    // Global parameters
    private int numNodes;
    private int minPerActive;
    private int maxPerActive;
    private int minSendDelay;
    private int snapshotDelay;
    private int maxNumber;

    // Node-specific parameters
    private int nodeID;
    private String host;
    private int port;
    private List<Integer> neighborIDs;
    private String configFilePath;

    // Node state
    private boolean isActive;
    private int totalMessagesSent = 0;
    private int messagesToSendInCurrentSession = 0;
    private int currentMessageNumberInSession = 0;
    private int maxMessagesAllowed;

    // Vector clock
    private Map<Integer, Integer> vectorClock;

    // Snapshot variables
    private int snapshotID = 0;
    private Set<Integer> activeSnapshots;
    private Map<Integer, Boolean> localStateRecorded;
    private Map<Integer, Set<Integer>> receivedMarkers;
    private Map<Integer, Map<Integer, List<String>>> channelStates;
    private Map<Integer, Map<Integer, Integer>> recordedVectorClocks;
    private Map<Integer, Integer> parentInSpanningTree;
    private Map<Integer, Map<Integer, Map<Integer, Integer>>> collectedVectorClocks;
    private Map<Integer, StringBuilder> collectedSnapshotData = new ConcurrentHashMap<>();
    private Map<Integer, Set<Integer>> snapshotDataReceivedFromMap;

    // Termination detection variables
    private Map<Integer, Map<Integer, String>> collectedActivityStates;
    private Map<Integer, Map<Integer, Map<Integer, Integer>>> collectedChannelStates;

    // Node connections and communication
    private Map<Integer, NodeInfo> nodeInfoMap;
    private Map<Integer, SctpChannel> neighborChannels;
    private SctpServerChannel serverChannel;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(50);

    // New message type for parent acknowledgment
    private static final String PARENT_ACK = "PARENT_ACK";

    // Data structures to keep track of parent and children
    private Map<Integer, Set<Integer>> children = new ConcurrentHashMap<>();

    // Output files
    private BufferedWriter outputFileWriter;
    private BufferedWriter consistencyCheckWriter;
    private int totalConsistencyChecks = 0;
    private int consistencyChecksPassed = 0;

    // Termination variables
    private final int terminationSleepTime = 500; // 0.5 seconds (used for debugging)
    private boolean isTerminated = false;

    // Snapshot initiation scheduler
    private ScheduledFuture<?> snapshotInitiationFuture;

    public static void main(String[] args) {
        Node node = new Node();
        node.initialize(args);
        node.start();
    }

    // Method to initialize the node
    private void initialize(String[] args) {
        if (args.length < 11) {
            System.out.println("Insufficient arguments provided.");
            System.exit(1);
        }

        try {
            // Parse global parameters
            numNodes = Integer.parseInt(args[0]);
            minPerActive = Integer.parseInt(args[1]);
            maxPerActive = Integer.parseInt(args[2]);
            minSendDelay = Integer.parseInt(args[3]);
            snapshotDelay = Integer.parseInt(args[4]);
            maxNumber = Integer.parseInt(args[5]);

            // Parse node-specific parameters
            nodeID = Integer.parseInt(args[6]);
            host = args[7];
            port = Integer.parseInt(args[8]);

            // Parse neighbor IDs from space-separated string in args[9]
            neighborIDs = new ArrayList<>();
            String[] neighborsArray = args[9].split("\\s+");
            for (String neighbor : neighborsArray) {
                if (!neighbor.isEmpty()) {
                    neighborIDs.add(Integer.parseInt(neighbor.trim()));
                }
            }

            // Parse the config file path
            configFilePath = args[10];

            // Print the hostname of the current machine
            try {
                String hostname = InetAddress.getLocalHost().getHostName();
                System.out.println(nodeID + ": Running on host: " + hostname);
            } catch (Exception e) {
                System.out.println(nodeID + ": Unable to get hostname.");
                e.printStackTrace();
            }

            // Parse the configuration file to get node info
            parseConfigFile();

            // Print global parameters
            System.out.println(nodeID + ": Global parameters:\n" +
                    nodeID + ": Number of Nodes: " + numNodes
                    + " || minPerActive: " + minPerActive
                    + " || maxPerActive: " + maxPerActive
                    + " || minSendDelay: " + minSendDelay
                    + " || snapshotDelay: " + snapshotDelay
                    + " || maxNumber: " + maxNumber);

            // Print node-specific parameters
            System.out.println(nodeID + ": Local parameters:\n" +
                    nodeID + ": Node ID: " + nodeID
                    + " || Host: " + host
                    + " || Port: " + port
                    + " || Neighbors: " + neighborIDs);

            // Initialize the node's active state
            isActive = (nodeID == 0); // Only node 0 is active at the start
            maxMessagesAllowed = maxNumber;

            // Initialize the vector clock
            vectorClock = new ConcurrentHashMap<>();
            for (int i = 0; i < numNodes; i++) {
                vectorClock.put(i, 0);
            }

            // Initialize snapshot variables
            localStateRecorded = new ConcurrentHashMap<>();
            receivedMarkers = new ConcurrentHashMap<>();
            channelStates = new ConcurrentHashMap<>();
            recordedVectorClocks = new ConcurrentHashMap<>();
            parentInSpanningTree = new ConcurrentHashMap<>();
            collectedVectorClocks = new ConcurrentHashMap<>();
            activeSnapshots = ConcurrentHashMap.newKeySet();
            snapshotDataReceivedFromMap = new ConcurrentHashMap<>();

            // Initialize termination detection variables
            collectedActivityStates = new ConcurrentHashMap<>();
            collectedChannelStates = new ConcurrentHashMap<>();

        } catch (Exception e) {
            System.out.println(nodeID + ": Error during initialization.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Method to parse the configuration file and populate nodeInfoMap
    private void parseConfigFile() {
        nodeInfoMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(configFilePath))) {
            String line;
            int nodesRead = 0;
            int numNodesInConfig = -1;

            // Skip lines until we find the first valid line (global parameters)
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || !line.matches("^\\d.*")) {
                    continue;
                }

                // Remove comments
                int commentIndex = line.indexOf('#');
                if (commentIndex != -1) {
                    line = line.substring(0, commentIndex).trim();
                }

                // Skip empty lines after removing comments
                if (line.isEmpty()) {
                    continue;
                }

                // Split tokens
                String[] tokens = line.split("\\s+");
                if (tokens.length >= 6) {
                    numNodesInConfig = Integer.parseInt(tokens[0]);
                    // We can parse global parameters from config file if needed
                    break;
                }
            }

            if (numNodesInConfig == -1) {
                throw new IOException("Invalid configuration file: missing global parameters.");
            }

            // Read the next numNodesInConfig lines to get node configurations
            while ((line = reader.readLine()) != null && nodesRead < numNodesInConfig) {
                line = line.trim();
                if (line.isEmpty() || !line.matches("^\\d.*")) {
                    continue;
                }

                // Remove comments
                int commentIndex = line.indexOf('#');
                if (commentIndex != -1) {
                    line = line.substring(0, commentIndex).trim();
                }

                // Skip empty lines after removing comments
                if (line.isEmpty()) {
                    continue;
                }

                // Split tokens
                String[] tokens = line.split("\\s+");
                if (tokens.length >= 3) {
                    int nodeId = Integer.parseInt(tokens[0]);
                    String hostName = tokens[1];
                    int portNumber = Integer.parseInt(tokens[2]);

                    nodeInfoMap.put(nodeId, new NodeInfo(hostName, portNumber));

                    nodesRead++;
                }
            }

        } catch (IOException e) {
            System.out.println(nodeID + ": Error parsing configuration file.");
            e.printStackTrace();
        }
    }

    // Method to start the node's execution
    private void start() {
        // Initialize neighbor channels
        neighborChannels = new ConcurrentHashMap<>();

        // Start the server to accept incoming connections
        startServer();

        // Connect to all neighbors
        connectToNeighbors();

        // Start the main protocol logic
        if (isActive) {
            System.out.println(nodeID + ": " + nodeID + " is ACTIVE at start.");
            executorService.execute(() -> startActiveProtocol());
        } else {
            System.out.println(nodeID + ": " + nodeID + " is PASSIVE at start.");
        }

        // Schedule periodic snapshot initiation for Node 0
        if (nodeID == 0) {
            snapshotInitiationFuture = executorService.scheduleAtFixedRate(() -> {
                if (!isTerminated && activeSnapshots.isEmpty()) {
                    initiateSnapshot();
                }
            }, snapshotDelay, snapshotDelay, TimeUnit.MILLISECONDS);
        }

        // Initialize output file writer for vector clocks
        String outputFileName = configFilePath.replace(".txt", "") + "-" + nodeID + ".out";
        try {
            outputFileWriter = new BufferedWriter(new FileWriter(outputFileName));
        } catch (IOException e) {
            System.out.println(nodeID + ": Error initializing output file writer.");
            e.printStackTrace();
        }

        // Initialize consistency check output file (only for Node 0)
        if (nodeID == 0) {
            try {
                // Determine the output directory
                String outputDir = new File(configFilePath).getParent();
                if (outputDir == null) {
                    outputDir = "."; // Use current directory if no parent directory is found
                }

                // Construct the full path for cons_checks.out
                String consistencyCheckFilePath = outputDir + File.separator + "cons_checks.out";

                // Open the file in write mode to overwrite previous runs
                consistencyCheckWriter = new BufferedWriter(new FileWriter(consistencyCheckFilePath)); // Overwrite mode
                System.out.println(nodeID + ": Consistency check output file initialized: " + consistencyCheckFilePath);
            } catch (IOException e) {
                System.out.println(nodeID + ": Error initializing consistency check output file.");
                e.printStackTrace();
            }
        }
    }

    // Method to start the server to accept incoming connections
    private void startServer() {
        try {
            serverChannel = SctpServerChannel.open();
            InetSocketAddress serverAddr = new InetSocketAddress(port);
            serverChannel.bind(serverAddr);
            System.out.println(nodeID + ": Server started on port " + port + ".");

            // Accept connections in a separate thread
            executorService.execute(() -> {
                while (true) {
                    try {
                        SctpChannel sctpChannel = serverChannel.accept();
                        // Read the neighbor's node ID
                        int neighborID = receiveNodeID(sctpChannel);

                        // Send my node ID back to the neighbor
                        sendNodeID(sctpChannel);

                        neighborChannels.put(neighborID, sctpChannel);
                        System.out.println(nodeID + ": Accepted connection from " + neighborID + ".");

                        // Start a thread to listen for messages from this neighbor
                        executorService.execute(() -> listenToNeighbor(sctpChannel, neighborID));
                    } catch (Exception e) {
                        System.out.println(nodeID + ": Error accepting connection.");
                        e.printStackTrace();
                        break;
                    }
                }
            });
        } catch (Exception e) {
            System.out.println(nodeID + ": Error starting server.");
            e.printStackTrace();
        }
    }

    // Method to connect to all neighbors
    private void connectToNeighbors() {
        for (int neighborID : neighborIDs) {
            if (neighborID != nodeID) {
                executorService.execute(() -> {
                    boolean connected = false;
                    int retryCount = 0;
                    int maxRetries = 500; // Increased to 500 retries
                    while (!connected && retryCount < maxRetries) {
                        try {
                            // Get neighbor host and port from nodeInfoMap
                            NodeInfo neighborInfo = nodeInfoMap.get(neighborID);
                            if (neighborInfo == null) {
                                throw new IOException("Neighbor " + neighborID + " not found in config file.");
                            }

                            InetSocketAddress neighborAddr = new InetSocketAddress(neighborInfo.host,
                                    neighborInfo.port);
                            SctpChannel sctpChannel = SctpChannel.open();
                            sctpChannel.connect(neighborAddr);
                            System.out.println(nodeID + ": Connected to " + neighborID + ".");

                            // Send my node ID to the neighbor
                            sendNodeID(sctpChannel);

                            // Receive the neighbor's node ID
                            int receivedNodeID = receiveNodeID(sctpChannel);

                            neighborChannels.put(neighborID, sctpChannel);

                            // Start a thread to listen for messages from this neighbor
                            executorService.execute(() -> listenToNeighbor(sctpChannel, neighborID));

                            connected = true;
                        } catch (Exception e) {
                            System.out.println(nodeID + ": Error connecting to " + neighborID + ". Retrying...");
                            retryCount++;
                            try {
                                Thread.sleep(1000); // Wait for 1 second before retrying
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    if (!connected) {
                        System.out.println(nodeID + ": Failed to connect to " + neighborID + " after " + maxRetries
                                + " retries.");
                    }
                });
            }
        }
    }

    // Method to start the active protocol
    private void startActiveProtocol() {
        if (isTerminated) {
            return;
        }        

        try {
            while (isActive && totalMessagesSent < maxMessagesAllowed) {
                // Decide how many messages to send in this active session
                messagesToSendInCurrentSession = getRandomNumber(minPerActive, maxPerActive);
                // Adjust messagesToSendInCurrentSession if it exceeds the maxMessagesAllowed
                if (totalMessagesSent + messagesToSendInCurrentSession > maxMessagesAllowed) {
                    messagesToSendInCurrentSession = maxMessagesAllowed - totalMessagesSent;
                }
                System.out.println(nodeID + ": " + nodeID + " will send " + messagesToSendInCurrentSession + " msgs.");

                int messagesSentInSession = 0;

                for (int i = 0; i < messagesToSendInCurrentSession && totalMessagesSent < maxMessagesAllowed;) {
                    // Randomly select a neighbor to send the message to
                    int neighborID = neighborIDs.get(new Random().nextInt(neighborIDs.size()));

                    currentMessageNumberInSession = messagesSentInSession + 1;

                    // Send the message
                    boolean sent = sendMessage(neighborID, currentMessageNumberInSession,
                            messagesToSendInCurrentSession, totalMessagesSent + 1);

                    if (sent) {
                        // Increment counts only if the message was successfully sent
                        messagesSentInSession++;
                        totalMessagesSent++;

                        i++; // Move to the next message
                    } else {
                        // If not sent, wait and retry
                        try {
                            Thread.sleep(1000); // Wait for 1 second before retrying
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    if (i < messagesToSendInCurrentSession && totalMessagesSent < maxMessagesAllowed) {
                        // Wait for at least minSendDelay before sending the next message
                        Thread.sleep(minSendDelay);
                    }
                }

                // Turn PASSIVE after sending messages
                isActive = false;

                // Determine the reason for becoming PASSIVE
                if (totalMessagesSent >= maxMessagesAllowed) {
                    System.out.println(nodeID + ": " + nodeID
                            + " is now PASSIVE. (Sent max number of messages: " + maxNumber + ")");
                } else {
                    System.out.println(nodeID + ": " + nodeID + " is now PASSIVE. (Sent msgs in current set: "
                            + messagesSentInSession + "/" + messagesToSendInCurrentSession + ", MaxPerActive:"
                            + maxPerActive + ")");
                }
            }
        } catch (Exception e) {
            System.out.println(nodeID + ": Error in active protocol.");
            e.printStackTrace();
        }
    }

    private void listenToNeighbor(SctpChannel sctpChannel, int neighborID) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16384); // Increased buffer size

            // Now enter the loop to listen for messages
            while (true) {
                buffer.clear();
                MessageInfo messageInfoApp = sctpChannel.receive(buffer, null, null);
                if (messageInfoApp != null) {
                    buffer.flip();
                    String message = new String(buffer.array(), buffer.position(), buffer.limit()).trim();
                    handleMessage(message, neighborID);
                }
            }
        } catch (Exception e) {
            System.out.println(nodeID + ": Connection to " + neighborID + " lost.");
            e.printStackTrace();
        }
    }

    // Method to handle incoming messages
    private synchronized void handleMessage(String message, int fromNodeID) {
        try {
            // Print the received message
            System.out.println(nodeID + ": Received message from " + fromNodeID + ": \"" + message + "\"");

            // Check if it's a termination message
            if (message.startsWith("TERMINATE")) {
                handleTerminationMessage();
                return;
            }

            if (isTerminated) {
                return;
            }

            if (message.startsWith("PARENT_ACK")) {
                String[] parts = message.split(":", 2);
                if (parts.length < 2) {
                    System.out.println(nodeID + ": Invalid PARENT_ACK message format from " + fromNodeID + ": " + message);
                    return;
                }
                int receivedSnapshotID = Integer.parseInt(parts[1]);
                handleParentAckMessage(fromNodeID, receivedSnapshotID);
                return;
            }

            // Check if it's a marker message
            if (message.startsWith("MARKER")) {
                String[] parts = message.split(":", 2);
                if (parts.length < 2) {
                    System.out.println(nodeID + ": Invalid MARKER message format from " + fromNodeID + ": " + message);
                    return;
                }
                int receivedSnapshotID = Integer.parseInt(parts[1]);
                handleMarkerMessage(fromNodeID, receivedSnapshotID);
                return;
            }

            // Check if it's snapshot data
            if (message.startsWith("SNAPSHOT")) {
                // Split the message into parts
                String[] parts = message.split(":", 3); // Limit to 3 parts
                if (parts.length < 3) {
                    System.out.println(nodeID + ": Invalid SNAPSHOT message format from " + fromNodeID + ": " + message);
                    return;
                }
                int receivedSnapshotID = Integer.parseInt(parts[1]);
                String data = parts[2]; // The rest is the data
                handleSnapshotData(fromNodeID, receivedSnapshotID, data);
                return;
            }

            // Deserialize the message to extract the vector clock
            String[] parts = message.split(";", 2);
            if (parts.length < 2) {
                System.out.println(nodeID + ": Invalid application message format from " + fromNodeID + ": " + message);
                return;
            }
            String msgContent = parts[0];
            Map<Integer, Integer> receivedVectorClock = deserializeVectorClock(parts[1]);

            // Print the received application message
            System.out.println(
                    nodeID + ": " + nodeID + " rec. from " + fromNodeID + ": " + msgContent + ".");

            // Prepare the vector clocks
            Map<Integer, Integer> localClockBeforeUpdate = new HashMap<>(vectorClock);
            Map<Integer, Integer> mergedVectorClock = mergeVectorClocks(vectorClock, receivedVectorClock);
            vectorClock = new HashMap<>(mergedVectorClock);
            vectorClock.put(nodeID, vectorClock.get(nodeID) + 1);

            // Now print the vector clocks as per instructions
            String prefix = nodeID + ": ------";
            System.out.println(prefix + " Received VC:    " + vectorClockToString(receivedVectorClock));
            System.out.println(prefix + " Old local VC:   " + vectorClockToString(localClockBeforeUpdate));
            System.out.println(prefix + " Merged VC:      " + vectorClockToString(mergedVectorClock));
            System.out.println(prefix + " New local VC:   " + vectorClockToString(vectorClock));

            // Activation logic
            if (!isActive && totalMessagesSent < maxMessagesAllowed) {
                isActive = true;
                System.out.println(nodeID + ": " + nodeID + " is now ACTIVE. (Received a message from "
                        + fromNodeID + ")");
                executorService.execute(() -> startActiveProtocol());
            } else if (totalMessagesSent >= maxMessagesAllowed) {
                System.out.println(nodeID + ": " + nodeID
                        + " remains PASSIVE. (Already sent max number of messages: " + maxNumber + ")");
            }

            // For each snapshot in progress, check if we need to record the message
            for (int snapID : activeSnapshots) {
                if (localStateRecorded.containsKey(snapID)
                        && !receivedMarkers.get(snapID).contains(fromNodeID)) {
                    // Record the message
                    // Ensure we only record messages if the snapshot is still active
                    System.out.println(nodeID + ": SS " + snapID + ": Recording message from " + fromNodeID
                            + " on channel state.");
                    channelStates.get(snapID).get(fromNodeID).add(message);
                }
            }
        } catch (Exception e) {
            System.out.println(nodeID + ": Error handling message from " + fromNodeID + ".");
            e.printStackTrace();
        }
    }


    // Method to send a message to a neighbor
    private boolean sendMessage(int neighborID, int messageNumber, int totalMessagesInSession,
            int totalMessagesSentSoFar) {
        try {
            SctpChannel sctpChannel = neighborChannels.get(neighborID);

            // Wait until the connection is established
            while (sctpChannel == null || !sctpChannel.isOpen()) {
                // Wait and retry
                Thread.sleep(1000); // Wait for 1 second before retrying
                sctpChannel = neighborChannels.get(neighborID);
            }

            // Prepare the message
            String msgContent = "Hi this is msg " + messageNumber + "/" + totalMessagesInSession + " (" + nodeID
                    + ": " + totalMessagesSentSoFar + "/" + maxNumber + " total)";

            // Before sending, get the old vector clock
            Map<Integer, Integer> oldVectorClock = new HashMap<>(vectorClock);

            // Increment the local clock
            vectorClock.put(nodeID, vectorClock.get(nodeID) + 1);

            // Serialize the vector clock
            String serializedVectorClock = serializeVectorClock(vectorClock);

            // Combine message and vector clock
            String fullMessage = msgContent + ";" + serializedVectorClock;

            // Send the message
            ByteBuffer buffer = ByteBuffer.allocate(16384); // Increased buffer size
            buffer.put(fullMessage.getBytes());
            buffer.flip();
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sctpChannel.send(buffer, messageInfo);

            // Print the message with vector clocks
            System.out.println(nodeID + ": " + nodeID + " to " + neighborID + ": msg " + messageNumber + "/"
                    + totalMessagesInSession + " (" + nodeID + ": " + totalMessagesSentSoFar + "/" + maxNumber
                    + " total).");

            // Now print the vector clocks as per instructions
            String prefix = nodeID + ": ------";
            System.out.println(prefix + " VC before send: " + vectorClockToString(oldVectorClock));
            System.out.println(prefix + " VC after send:  " + vectorClockToString(vectorClock));

            return true; // Message sent successfully
        } catch (Exception e) {
            System.out.println(nodeID + ": Error sending msg to " + neighborID + ".");
            e.printStackTrace();
            return false; // Message not sent
        }
    }

    // Method to send my node ID to the neighbor upon connection
    private void sendNodeID(SctpChannel sctpChannel) {
        try {
            String nodeIDStr = String.valueOf(nodeID);
            ByteBuffer buffer = ByteBuffer.allocate(16384);
            buffer.put(nodeIDStr.getBytes());
            buffer.flip();
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sctpChannel.send(buffer, messageInfo);
        } catch (Exception e) {
            System.out.println(nodeID + ": Error sending node ID.");
            e.printStackTrace();
        }
    }

    // Method to receive the neighbor's node ID upon connection
    private int receiveNodeID(SctpChannel sctpChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16384);
            MessageInfo messageInfo = sctpChannel.receive(buffer, null, null);
            if (messageInfo != null) {
                buffer.flip();
                String nodeIDStr = new String(buffer.array(), buffer.position(), buffer.limit()).trim();
                return Integer.parseInt(nodeIDStr);
            }
        } catch (Exception e) {
            System.out.println(nodeID + ": Error receiving node ID.");
            e.printStackTrace();
        }
        return -1;
    }

    private synchronized void initiateSnapshot() {
        snapshotID++;
        System.out.println(nodeID + ": SS " + snapshotID + ": Initiating snapshot.");
        activeSnapshots.add(snapshotID);
        recordLocalState(snapshotID);
    
        // Initialize receivedMarkers set for this snapshot
        receivedMarkers.put(snapshotID, ConcurrentHashMap.newKeySet());
    
        // Send markers to all neighbors
        sendMarkerMessages(snapshotID);// send to all neightbors
    }
    
    private synchronized void handleMarkerMessage(int fromNodeID, int receivedSnapshotID) {
        if (!localStateRecorded.containsKey(receivedSnapshotID)) {
            if (!parentInSpanningTree.containsKey(receivedSnapshotID)) {
                parentInSpanningTree.put(receivedSnapshotID, fromNodeID);
            }
            System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Received first MARKER from " + fromNodeID + ".");
    
            // Send PARENT_ACK to confirm parent-child relationship
            sendControlMessage(fromNodeID, "PARENT_ACK:" + receivedSnapshotID);
    
            // Record local state and send markers
            System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Recording local state and sending out markers.");
            activeSnapshots.add(receivedSnapshotID);
            recordLocalState(receivedSnapshotID);
    
            // Initialize receivedMarkers set and add fromNodeID
            receivedMarkers.put(receivedSnapshotID, ConcurrentHashMap.newKeySet());
            receivedMarkers.get(receivedSnapshotID).add(fromNodeID);
    
            // Send markers to all neighbors except parent (fromNodeID)
            for (int neighborID : neighborIDs) {
                if (neighborID != fromNodeID) {
                    System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Sending MARKER to " + neighborID + ".");
                    sendControlMessage(neighborID, "MARKER:" + receivedSnapshotID);
                }
            }
    
            // Initialize children set for this snapshot
            children.put(receivedSnapshotID, ConcurrentHashMap.newKeySet());
        } else {
            System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Received subsequent MARKER from " + fromNodeID + ".");
            receivedMarkers.get(receivedSnapshotID).add(fromNodeID);
        }
    
        // Check if we have received markers from all neighbors
        if (receivedMarkers.get(receivedSnapshotID).size() == neighborIDs.size()) {
            // All markers received from all neighbors
            // If no children, send snapshot data to parent or collect at root
            if (children.get(receivedSnapshotID).isEmpty()) {
                if (nodeID != 0) {
                    System.out.println(nodeID + ": SS " + receivedSnapshotID + ": All markers received and no children. Sending snapshot data to parent.");
                    sendRecordedStateToParent(receivedSnapshotID);
                    cleanupSnapshotData(receivedSnapshotID);
                } else {
                    System.out.println(nodeID + ": SS " + receivedSnapshotID + ": All markers received. Collecting snapshot states.");
                    collectSnapshotStates(receivedSnapshotID);
                    cleanupSnapshotData(receivedSnapshotID);
                }
            }
            // Else, wait for snapshot data from children
        }
    }   
    
    private synchronized void handleParentAckMessage(int fromNodeID, int receivedSnapshotID) {
        System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Received PARENT_ACK from child " + fromNodeID + ".");
        children.computeIfAbsent(receivedSnapshotID, k -> ConcurrentHashMap.newKeySet()).add(fromNodeID);
    }    

    private synchronized void recordLocalState(int snapshotID) {
        localStateRecorded.put(snapshotID, true);
    
        channelStates.put(snapshotID, new ConcurrentHashMap<>());
        recordedVectorClocks.put(snapshotID, new HashMap<>(vectorClock));
    
        // Initialize channel states
        for (int neighborID : neighborIDs) {
            channelStates.get(snapshotID).put(neighborID, new ArrayList<>());
        }
    
        System.out.println(nodeID + ": SS " + snapshotID + ": Recorded local state with vector clock "
                + vectorClockToString(recordedVectorClocks.get(snapshotID)) + ".");
    
        // Write the vector clock to the output file
        writeVectorClockToOutputFile(recordedVectorClocks.get(snapshotID));
    }
    
    private void sendMarkerMessages(int snapshotID) {
        int parentID = parentInSpanningTree.getOrDefault(snapshotID, -1);
        for (int neighborID : neighborIDs) {
            if (neighborID != parentID) {
                System.out.println(nodeID + ": SS " + snapshotID + ": Sending MARKER to " + neighborID + ".");
                sendControlMessage(neighborID, "MARKER:" + snapshotID);
            }
        }
    }    
    
    private void sendControlMessage(int neighborID, String message) {
        try {
            SctpChannel sctpChannel = neighborChannels.get(neighborID);

            ByteBuffer buffer = ByteBuffer.allocate(16384); // Increased buffer size
            buffer.put(message.getBytes());
            buffer.flip();
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sctpChannel.send(buffer, messageInfo);
        } catch (Exception e) {
            System.out.println(nodeID + ": Error sending control msg to " + neighborID + ".");
            e.printStackTrace();
        }
    }

    private synchronized void sendRecordedStateToParent(int snapshotID) {
        int parentID = parentInSpanningTree.get(snapshotID);
        StringBuilder serializedData = new StringBuilder();
    
        // Add own recorded state
        Map<Integer, Integer> vc = recordedVectorClocks.get(snapshotID);
        serializedData.append(nodeID).append(": ").append(formatVectorClockForOutputFile(vc)).append("\n");
        serializedData.append("ACTIVITY: ").append(isActive ? "ACTIVE" : "PASSIVE").append("\n");
    
        // Add own channel states
        Map<Integer, List<String>> channels = channelStates.get(snapshotID);
        for (Map.Entry<Integer, List<String>> entry : channels.entrySet()) {
            int channelFrom = entry.getKey();
            int messageCount = entry.getValue().size();
            serializedData.append("CHANNEL_FROM:").append(channelFrom).append(":MESSAGE_COUNT:")
                    .append(messageCount).append("\n");
        }
    
        // Append collected data from children
        StringBuilder collectedData = collectedSnapshotData.get(snapshotID);
        if (collectedData != null) {
            serializedData.append("\n").append(collectedData.toString());
        }
    
        System.out.println(nodeID + ": SS " + snapshotID + ": Sending recorded state to parent " + parentID
                + " with collected data:\n" + serializedData.toString());
    
        // Send the aggregated data to the parent
        sendControlMessage(parentID, "SNAPSHOT:" + snapshotID + ":" + serializedData.toString());
    }      

    private Map<Integer, Set<Integer>> snapshotDataReceivedFrom = new ConcurrentHashMap<>();

    private synchronized void handleSnapshotData(int fromNodeID, int receivedSnapshotID, String data) {
        System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Received SNAPSHOT data from child " + fromNodeID + ".");
        System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Snapshot data received:\n" + data);
    
        // Append the data received from the child to collectedSnapshotData with a newline
        collectedSnapshotData.computeIfAbsent(receivedSnapshotID, k -> new StringBuilder())
                .append(data).append("\n");
    
        // Record that we have received snapshot data from this child
        snapshotDataReceivedFrom.computeIfAbsent(receivedSnapshotID, k -> ConcurrentHashMap.newKeySet())
                .add(fromNodeID);
    
        // Check if we have received snapshot data from all children
        Set<Integer> expectedChildren = children.get(receivedSnapshotID);
        Set<Integer> receivedFrom = snapshotDataReceivedFrom.get(receivedSnapshotID);
    
        if (expectedChildren != null && receivedFrom != null && expectedChildren.equals(receivedFrom)) {
            // Received from all children
            if (nodeID != 0) {
                System.out.println(nodeID + ": SS " + receivedSnapshotID + ": Received snapshot data from all children. Sending snapshot data to parent.");
                sendRecordedStateToParent(receivedSnapshotID);
                cleanupSnapshotData(receivedSnapshotID);
            } else {
                System.out.println(nodeID + ": SS " + receivedSnapshotID + ": All snapshot data received. Collecting snapshot states.");
                collectSnapshotStates(receivedSnapshotID);
                cleanupSnapshotData(receivedSnapshotID);
            }
        }
    }

    private synchronized void collectSnapshotStates(int snapshotID) {
        System.out.println(nodeID + ": SS " + snapshotID + ": Collecting snapshot states.");
    
        Map<Integer, Map<Integer, Integer>> nodeVectorClocks = new HashMap<>();
        Map<Integer, String> nodeActivityStates = new HashMap<>();
        Map<Integer, Map<Integer, Integer>> nodeChannelStates = new HashMap<>();
    
        // Add own vector clock and activity state
        nodeVectorClocks.put(nodeID, recordedVectorClocks.get(snapshotID));
        nodeActivityStates.put(nodeID, isActive ? "ACTIVE" : "PASSIVE");
    
        // Add own channel states
        Map<Integer, List<String>> ownChannels = channelStates.get(snapshotID);
        Map<Integer, Integer> ownChannelMessageCounts = new HashMap<>();
        for (Map.Entry<Integer, List<String>> entry : ownChannels.entrySet()) {
            int channelFrom = entry.getKey();
            int messageCount = entry.getValue().size();
            ownChannelMessageCounts.put(channelFrom, messageCount);
        }
        nodeChannelStates.put(nodeID, ownChannelMessageCounts);
    
        // Parse collected snapshot data from children
        StringBuilder collectedData = collectedSnapshotData.get(snapshotID);
        if (collectedData != null) {
            String[] lines = collectedData.toString().split("\n");
            int currentNodeID = -1;
    
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
    
                if (line.matches("^\\d+:.*")) {
                    // Node ID and vector clock line
                    String[] parts = line.split(":", 2);
                    currentNodeID = Integer.parseInt(parts[0].trim());
                    Map<Integer, Integer> vc = deserializeVectorClockForVerification(parts[1].trim());
                    nodeVectorClocks.put(currentNodeID, vc);
                } else if (line.startsWith("ACTIVITY:")) {
                    // Activity state
                    String activityState = line.substring("ACTIVITY:".length()).trim();
                    nodeActivityStates.put(currentNodeID, activityState);
                } else if (line.startsWith("CHANNEL_FROM:")) {
                    // Channel state
                    String[] channelParts = line.split(":");
                    if (channelParts.length >= 4) {
                        int channelFrom = Integer.parseInt(channelParts[1]);
                        int messageCount = Integer.parseInt(channelParts[3]);
    
                        nodeChannelStates.computeIfAbsent(currentNodeID, k -> new HashMap<>())
                            .put(channelFrom, messageCount);
                    }
                }
            }
        }
    
        // Print the collected vector clocks
        System.out.println(nodeID + ": SS " + snapshotID + ": Collected vector clocks:");
        for (Map.Entry<Integer, Map<Integer, Integer>> entry : nodeVectorClocks.entrySet()) {
            int nodeId = entry.getKey();
            Map<Integer, Integer> vc = entry.getValue();
            System.out.println(nodeID + ": SS " + snapshotID + ": Node " + nodeId + ": "
                    + vectorClockToString(vc));
        }
    
        // Consistency check
        System.out.println(nodeID + ": SS " + snapshotID + ": CONSISTENCY check beginning...");
        totalConsistencyChecks++;
        boolean isConsistent = verifySnapshotConsistency(nodeVectorClocks);
        if (isConsistent) {
            consistencyChecksPassed++;
            System.out.println(nodeID + ": SS " + snapshotID + ": Confirmed: SNAPSHOT is CONSISTENT.");
        } else {
            System.out.println(nodeID + ": SS " + snapshotID + ": SNAPSHOT is NOT CONSISTENT.");
        }
    
        // Write consistency check result to output file
        writeConsistencyCheckResult();
    
        // Check for termination based on MAP protocol
        if (isConsistent && checkForTermination(nodeActivityStates, nodeChannelStates)) {
            System.out.println(nodeID + ": Termination detected based on MAP protocol.");
            sendTerminationMessage();
        }
    }
    
    private boolean checkForTermination(Map<Integer, String> activityStates, Map<Integer, Map<Integer, Integer>> channelStates) {
        // Check if all nodes are passive
        for (String state : activityStates.values()) {
            if ("ACTIVE".equals(state)) {
                return false;
            }
        }
    
        // Check if all channels are empty
        for (Map<Integer, Integer> channels : channelStates.values()) {
            for (int count : channels.values()) {
                if (count > 0) {
                    return false;
                }
            }
        }
    
        return true;
    }
    
     
    private void sendTerminationMessage() {
        handleTerminationMessage();
    }    

    // method to handle the termination message
    private void handleTerminationMessage() {
        if (isTerminated) {
            return;
        }
    
        // Send TERMINATE message to all neighbors
        for (int neighborID : neighborIDs) {
            sendControlMessage(neighborID, "TERMINATE");
        }
    
        isTerminated = true;
        System.out.println(nodeID + ": Halt order received from node 0...");
    
        // Cancel the snapshot initiation task if it's scheduled
        if (snapshotInitiationFuture != null && !snapshotInitiationFuture.isCancelled()) {
            snapshotInitiationFuture.cancel(false);
        }
    
        System.out.println(nodeID + ": Sleeping for " + (terminationSleepTime / 1000 / 60)
                + " minutes before halting.");
        try {
            Thread.sleep(terminationSleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println(nodeID + ": Node has halted.");
        shutdown(); // Shutdown the node
    }
    

    // Method to write consistency check result to output file
    private void writeConsistencyCheckResult() {
        if (nodeID == 0) {
            if (consistencyCheckWriter != null) {
                try {
                    String result = consistencyChecksPassed + "/" + totalConsistencyChecks
                            + " consistency checks passed.";
                    consistencyCheckWriter.write(result);
                    consistencyCheckWriter.newLine();
                    consistencyCheckWriter.flush();
                    System.out.println(nodeID + ": Consistency check result written to file: " + result);
                } catch (IOException e) {
                    System.out.println(nodeID + ": Error writing to consistency check output file.");
                    e.printStackTrace();
                }
            } else {
                System.out.println(nodeID + ": consistencyCheckWriter is null!");
            }
        }
    }

    // Method to verify snapshot consistency
    private boolean verifySnapshotConsistency(Map<Integer, Map<Integer, Integer>> nodeVectorClocks) {
        for (int i = 0; i < numNodes; i++) {
            Map<Integer, Integer> VC_i = nodeVectorClocks.get(i);
            if (VC_i == null)
                continue; // Node i's vector clock is not available
            for (int j = 0; j < numNodes; j++) {
                Map<Integer, Integer> VC_j = nodeVectorClocks.get(j);
                if (VC_j == null)
                    continue; // Node j's vector clock is not available
                if (VC_i.get(j) > VC_j.get(j)) {
                    // Inconsistent
                    return false;
                }
            }
        }
        // Consistent
        return true;
    }

    // Method to write vector clock to output file
    private void writeVectorClockToOutputFile(Map<Integer, Integer> vc) {
        try {
            String vectorClockStr = formatVectorClockForOutputFile(vc);
            outputFileWriter.write(vectorClockStr);
            outputFileWriter.newLine();
            outputFileWriter.flush();
        } catch (IOException e) {
            System.out.println(nodeID + ": Error writing to output file.");
            e.printStackTrace();
        }
    }

    // Utility method to format vector clock for output file
    private String formatVectorClockForOutputFile(Map<Integer, Integer> vc) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numNodes; i++) {
            sb.append(vc.get(i));
            if (i < numNodes - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    // Utility method to get a random number between min and max (inclusive)
    private int getRandomNumber(int min, int max) {
        return new Random().nextInt((max - min) + 1) + min;
    }

    private void shutdown() {
        try {
            isActive = false;
            executorService.shutdownNow();
            if (serverChannel != null && serverChannel.isOpen()) {
                serverChannel.close();
            }
            for (SctpChannel channel : neighborChannels.values()) {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            }
            if (outputFileWriter != null) {
                outputFileWriter.close();
            }
            if (consistencyCheckWriter != null) {
                consistencyCheckWriter.close();
            }
            System.out.println(nodeID + ": Node has been shut down.");
            System.exit(0); // Ensure the program exits
        } catch (Exception e) {
            System.out.println(nodeID + ": Error during shutdown.");
            e.printStackTrace();
        }
    }    

    // Method to serialize the vector clock to a string
    private String serializeVectorClock(Map<Integer, Integer> vc) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numNodes; i++) {
            sb.append(vc.get(i));
            if (i < numNodes - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    // Method to deserialize the vector clock from a string
    private Map<Integer, Integer> deserializeVectorClock(String str) {
        Map<Integer, Integer> vc = new HashMap<>();
        String[] parts = str.split(",");
        for (int i = 0; i < parts.length; i++) {
            vc.put(i, Integer.parseInt(parts[i]));
        }
        return vc;
    }

    // Method to deserialize vector clock from space-separated string for verification
    private Map<Integer, Integer> deserializeVectorClockForVerification(String str) {
        Map<Integer, Integer> vc = new HashMap<>();
        String[] parts = str.trim().split("\\s+");
        for (int i = 0; i < parts.length; i++) {
            vc.put(i, Integer.parseInt(parts[i]));
        }
        return vc;
    }

    // Method to merge two vector clocks
    private Map<Integer, Integer> mergeVectorClocks(Map<Integer, Integer> vc1, Map<Integer, Integer> vc2) {
        Map<Integer, Integer> merged = new HashMap<>();
        for (int i = 0; i < numNodes; i++) {
            merged.put(i, Math.max(vc1.get(i), vc2.get(i)));
        }
        return merged;
    }

    // Method to convert vector clock to string for printing
    private String vectorClockToString(Map<Integer, Integer> vc) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numNodes; i++) {
            sb.append(vc.get(i));
            if (i < numNodes - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    // Method to serialize recorded state
    private String serializeRecordedState(int snapshotID) {
        Map<Integer, Integer> vc = recordedVectorClocks.get(snapshotID);
        return vectorClockToString(vc);
    }

    private void cleanupSnapshotData(int snapshotID) {
        activeSnapshots.remove(snapshotID);
        localStateRecorded.remove(snapshotID);
        receivedMarkers.remove(snapshotID);
        channelStates.remove(snapshotID);
        recordedVectorClocks.remove(snapshotID);
        parentInSpanningTree.remove(snapshotID);
        collectedSnapshotData.remove(snapshotID);
        snapshotDataReceivedFrom.remove(snapshotID);
        children.remove(snapshotID);
    }
    

    // Inner class to store node information
    private static class NodeInfo {
        String host;
        int port;

        NodeInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

}