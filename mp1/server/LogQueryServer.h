#pragma once

#include <boost/asio.hpp>

#include "LogQuerySession.h"

class LogQueryServer
{
public:
    LogQueryServer(boost::asio::io_service& io_service, int port)
        : acceptor_(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
        , socket_(io_service)
    {
        Accept();
    }

private:
    void Accept();

    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket socket_;
};