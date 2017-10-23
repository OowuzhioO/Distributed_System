read    -p "Username: " username
read -s -p "Password: " password && echo 

for x in {1..10} 
do
while [[ ${#x} -lt 2 ]] ; do
        x="0${x}"
done
sshpass -p $password ssh -tty -o StrictHostKeyChecking=no "${username}@fa17-cs425-g48-${x}.cs.illinois.edu"  \
"cd cs425-MPs/mp1/log && cmake . && make && ./logger machine.${x}.small.log && exit"
sshpass -p $password scp ${username}@fa17-cs425-g48-${x}.cs.illinois.edu:~/cs425-MPs/mp1/log/machine.${x}.small.log ../log_check/
done
