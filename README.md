# FALL2024-chandy-lamport-fidge-mattern
Implement a config-driven distributed system of n nodes communicating over persistent bidirectional FIFO sockets (TCP/SCTP) that run the MAP active/passive messaging protocol, use Chandy–Lamport snapshots, plus Fidge/Mattern vector clocks to detect and verify MAP termination, and then execute a protocol to bring all nodes to a halt.


Script code by Justin A and Jordan F

Instructions:
1. run:
  chmod +x build.sh launcher.sh cleanup.sh cleanFiles.sh
2. Expected execution order is:
  ./build.sh
  ./launcher.sh
  ./cleanup.sh
  ./cleanFiles.sh
3. I included 3 configs for 3, 5, and 7 nodes which should run correctly.
4. Once you update launcher.sh and cleanup.sh as shown below, it should run without issue as long as the scripts, config.txt, and Node.java are in the same folder
5. Please contact me if you have any issue.




launcher.sh and cleanup.sh do not need any modifications other than what the default options of these files (provided by the professor) require. Shown below:

launcher.sh ----------------------------------
1. Follow steps 1-4 in "Passwordless Login Instructions".
2. Change your net ID, root directory, and config file path launcher.sh
3. Run this script [from a dcxx machine] to launch terminals to other machines.

cleanup.sh ----------------------------------
1. Follow steps 1-4 in "Passwordless Login Instructions".
2. Change your net ID, root directory, and config file path cleanup.sh
3. Run script; this script is designed to issue kill commands to erroneous processes.

build.sh does not need any special instruction

cleanFiles.sh ----------------------------------
1. WARNING: this will delete all files matching the extensions in the folder of execution indiscriminantly.
   If you have important files with the same extension, you will want to more heavily alter the rm commands.
