read    -p "Username: " username
read -s -p "Password: " password && echo 

> hostnames.txt
for x in {1..10} 
do
	while [[ ${#x} -lt 2 ]] ; do
		x="0${x}"
	done
	echo "${username}@fa17-cs425-g48-${x}.cs.illinois.edu" >> hostnames.txt
done

sshpass -p $password pssh -h hostnames.txt -A -x '-tt' -i "-O StrictHostKeyChecking=no" -l root \
"echo ${password} | sudo -S yum -y install ${1}"
