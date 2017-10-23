#include "LogQueryServer.h"

#include <boost/asio.hpp>

namespace
{
    constexpr auto DEFAULT_PORT = 1867;
}

int main(int argc, char* argv[])
{
    try
    {
        boost::asio::io_service io_service;
        // Start the server with the port to listen on.
        LogQueryServer server(io_service, DEFAULT_PORT);
        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << "\n";
    }
    return 0;
}