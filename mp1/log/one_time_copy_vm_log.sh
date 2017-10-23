read    -p "Username: " username
read -s -p "Password: " password && echo 

for x in {1..10} 
do
y=${x}
while [[ ${#x} -lt 2 ]] ; do
        x="0${x}"
done
sshpass -p $password scp ../log_check/vm${y}.log ${username}@fa17-cs425-g48-${x}.cs.illinois.edu:~/cs425-MPs/mp1/log/
done
