#include <fstream>
#include <iostream>
#include <limits.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
	char hostname[HOST_NAME_MAX];
	gethostname(hostname, HOST_NAME_MAX);
	std::string real_hostname(hostname);
	
	
	char* file_name;
	try {
		file_name = argv[1];
	} catch(...) {
		std::cout << "Pass an argument indicate file name." << std::endl;
		return -1;
	}
	std::ofstream log_file;	
	log_file.open(file_name);
	int length = 15;


	std::srand((unsigned)time(0)); 		
	int current_year = 1939;
	//Structure: action follwed by ipv4 addr followed by time
	for (int len_num = 0; len_num < (rand()%length)+5; len_num++) {
		if (real_hostname.find("10") != std::string::npos) 
			break;

		std::string random_action = (((rand()%2)<1) ? "GET: " : "SET: "); //though I'm not sure what's set url
		std::string random_IP = "";
		for (int octet_num = 0; octet_num < 4; octet_num++) {
			int random_octet = rand()%256;
			random_IP += std::to_string(random_octet);
			if (octet_num != 3) 
				random_IP += '.';
		}

		std::string random_date = "";
		random_date+=std::to_string(rand()%12+1)+'/';
		random_date+=std::to_string(rand()%28+1)+'/'; // Not complicate the problem of Feburary
		if (current_year < 2017-length)
			current_year = (current_year + rand()%(2017-length-current_year));
		current_year++;
		random_date+=std::to_string(current_year);
		log_file << random_action << random_IP << " [" << (random_date) << "]" << std::endl;

		if (real_hostname.find("09") != std::string::npos) 
			break;
	}
	log_file.close();
	return 0;
}
