#include <chrono>
#include <vector>

#include "../client/LogQueryClient.h"

using boost::asio::ip::tcp;
using std::chrono::high_resolution_clock;

int main()
{
    try
    {
        boost::asio::io_service io_service;

        // Hardcode the command to consistently get results
        std::string command = "grep NASA benchmark*.log";

        std::vector<std::shared_ptr<LogQueryClient>> clients;
        // Begin timing when the client start initiating connections. 4 clients are created
        // to correspond to the 4 benchmarking servers (2 - 5)
        auto start = high_resolution_clock::now();
        for (int i = 2; i <= 5; ++i)
        {
            clients.emplace_back(std::make_shared<LogQueryClient>(io_service, command, i));
        }

        io_service.run();
        // Stop the timer when all of the commands have returned
        auto end = high_resolution_clock::now();

        // Calculate the latency in seconds
        auto query_latency = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
        std::cout << "Query latency: " << query_latency.count() << " seconds\n";

        for (auto& client : clients)
        {
            client->Close();
        }
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}