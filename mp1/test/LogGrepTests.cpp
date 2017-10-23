#include <iostream>
#include <string>
#include <vector>
#include <future>

#include "../client/LogQueryClient.h"

using boost::asio::ip::tcp;
using std::cout;
using std::endl;
using std::make_shared;
using std::string;
using std::vector;
using std::shared_ptr;
using std::to_string;

namespace
{
    // Performs an arbitrary grep on empty log files. Expect nothing.
    void TestEmptyFiles()
    {
        cout << "TestEmptyFiles: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep arbitrary test_logs/empty/empty";
        vector<std::future<void>> threads;
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
            threads.emplace_back(std::async(std::launch::async, [&io_service]{io_service.run();}));
        }
        //io_service.run();
        for (auto& thread : threads)
        {
            thread.get();
        }
        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 0 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Queries the logs for a rare pattern in a single log
    void TestOneMachineRarePattern()
    {
        cout << "TestOneMachineRarePattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" test_logs/one_rare/one_rare";
        vector<std::future<void>> threads;
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
            threads.emplace_back(std::async(std::launch::async, [&io_service]{io_service.run();}));
        }
        //io_service.run();
        for (auto& thread : threads)
        {
            thread.get();
        }
        
        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        
        cout << (total == 1 && clients[2]->line_count() == 1 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for multiple occurences in a single log
    void TestOneMachineSomewhatFrequentPattern()
    {
        cout << "TestOneMachineSomewhatFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/one_somewhat_frequent/one_somewhat_frequent";
        vector<std::future<void>> threads;
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
            threads.emplace_back(std::async(std::launch::async, [&io_service]{io_service.run();}));
        }
        //io_service.run();
        for (auto& thread : threads)
        {
            thread.get();
        }

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        
        cout << (total == 4 && clients[5]->line_count() == 4 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for many occurences in a single log
    void TestOneMachineFrequentPattern()
    {
        cout << "TestOneMachineFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/one_frequent/one_frequent";
        vector<std::future<void>> threads;
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
            threads.emplace_back(std::async(std::launch::async, [&io_service]{io_service.run();}));
        }
        //io_service.run();
        for (auto& thread : threads)
        {
            thread.get();
        }

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }

        cout << (total == 77 && clients[7]->line_count() == 77 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for a rare pattern in multiple logs
    void TestSomeMachinesRarePattern()
    {
        cout << "TestSomeMachinesRarePattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/some_rare/some_rare";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 5 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for multiple occurences in several logs
    void TestSomeMachinesSomewhatFrequentPattern()
    {
        cout << "TestSomeMachinesSomewhatFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/some_somewhat_frequent/some_somewhat_frequent";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 12 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for a frequent pattern in several machines
    void TestSomeMachinesFrequentPattern()
    {
        cout << "TestSomeMachinesFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/some_frequent/some_frequent";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 385 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for a single pattern in all logs
    void TestAllMachinesRarePattern()
    {
        cout << "TestAllMachinesRarePattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/all_rare/all_rare";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 10 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Looks for a few common patterns in all logs
    void TestAllMachinesSomwhatFrequentPattern()
    {
        cout << "TestAllMachinesSomwhatFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"The quick brown fox jumps over the lazy dog\" "
                          "test_logs/all_somewhat_frequent/all_somewhat_frequent";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 40 ? "PASS" : "FAIL");
        cout << endl;
    }

    // Counts the entire logs for every machine
    void TestAllMachinesFrequentPattern()
    {
        cout << "TestAllMachinesFrequentPattern: ";

        boost::asio::io_service io_service;
        vector<shared_ptr<LogQueryClient>> clients;
        string command = "grep \"\\.*\" "
                          "test_logs/all_frequent/all_frequent";
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(make_shared<LogQueryClient>(io_service, command + to_string(i) + ".log", i));
        }
        io_service.run();

        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            total += line_count;
            client->Close();
        }
        cout << (total == 80 ? "PASS" : "FAIL");
        cout << endl;
    }
}

int main(int argc, char* argv[])
{
    TestEmptyFiles();

    TestOneMachineRarePattern();
    TestOneMachineSomewhatFrequentPattern();
    TestOneMachineFrequentPattern();
    
    TestSomeMachinesRarePattern();
    TestSomeMachinesSomewhatFrequentPattern();
    TestSomeMachinesFrequentPattern();
    
    TestAllMachinesRarePattern();
    TestAllMachinesSomwhatFrequentPattern();
    TestAllMachinesFrequentPattern();
}