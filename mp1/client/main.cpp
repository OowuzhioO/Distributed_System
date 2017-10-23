#include <vector>

#include "LogQueryClient.h"

using boost::asio::ip::tcp;
using std::cout;

int main(int argc, char* argv[])
{
    try
    {
        boost::asio::io_service io_service;

        //Build the valid grep command (with any supplied options/flags)
        int required_components = 2;
        std::string command = "grep";
        for (int i = 1; i < argc; ++i)
        {
            // We need at least two arguments that aren't flags (pattern and file)
            if (argv[i][0] != '-')
            {
                --required_components;
            }
            command += ' ' + std::string(argv[i]);
        }
        // Terminate if the command is invalid
        if (required_components > 0)
        {
            cout << "Missing pattern or file. Exiting...\n";
            return 1;
        }

        // Will segfault if it's not a shared_ptr
        std::vector<std::shared_ptr<LogQueryClient>> clients;
        for (int i = 1; i <= 10; ++i)
        {
            clients.emplace_back(std::make_shared<LogQueryClient>(io_service, command, i));
        }

        io_service.run();

        // Print the results to the console and clean up the sockets
        int total = 0;
        for (auto& client : clients)
        {
            auto line_count = client->line_count();
            cout << "vm" + std::to_string(client->host_id()) + ": " << line_count << '\n';
            total += line_count;
            client->Close();
        }
        cout << "Total: " << total << std::endl;
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}