The project will build a p2p system based on UDP communication. All the machine will be set on a virtual ring and talk to part of the machines. However, each machine will known all other machines conditions. Each system (ring) will have one introducer and much more followers. 
    
    1. Run the DistributedGroupMember.jar and set one of the machine as the introducer through java -jar D*.java introducer
    2. Run the D*.jar and set other machine as the follower. For each follower, input introducer's ip address'
    3. Input "join" before excute any command, all the machines should join first. Introducer should join the system first
    4. Run the command "list membership list" to show membership list
    5. Run the command "list id" to show local machine ip address and attending time(timestamp)
    6. Run the command "leave" to leave from the system
