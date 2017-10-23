#include "LogQueryServer.h"

// Listens for a client connections then starts a session to run a command
void LogQueryServer::Accept()
{
    acceptor_.async_accept(socket_, 
        [this] (boost::system::error_code error)
        {
            if (!error)
            {
                // Create a new session to perform the log query on
                std::make_shared<LogQuerySession>(std::move(socket_))->ReadCommand();
            }
            Accept();
        });
}