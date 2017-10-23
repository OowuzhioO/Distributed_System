#pragma once

#include <array>
#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <string>

constexpr auto STREAM_INCREMENT = 128;
constexpr auto HEADER_SIZE = 8;

class LogQuerySession : public std::enable_shared_from_this<LogQuerySession>
{
public:
    LogQuerySession(boost::asio::ip::tcp::socket&& socket) : socket_(std::move(socket)) {}
    void ReadCommand();

private:
    // Actions to perform after receiving a command
    void ExecuteCommand(const std::string& command);
    void WriteOutputSize();
    void WriteOutputBody();
    void WriteOutputCount();

    // Encoding functions to send fixed size data representing numbers
    void EncodeHeader(int result_size);
    std::array<char, HEADER_SIZE> EncodeLineCount(const std::string& line_count);

    boost::asio::ip::tcp::socket socket_;

    // The command to be received and processed
    std::array<char, 1024> command_;
    std::string stripped_command_;
    
    // Temporarily holds the data to send that represents the result size
    std::array<char, HEADER_SIZE> header_;
    
    // Result string to hold the data to send
    std::string result_;
};