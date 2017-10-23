#pragma once

#include <boost/asio.hpp>
#include <fstream>
#include <vector>

constexpr auto HEADER_SIZE = 8;

class LogQueryClient
{
public:
    LogQueryClient(boost::asio::io_service& io_service, const std::string& command, int host_id);

    // Retrieve the lines from the grep output
    inline int line_count() { return line_count_; }
    // VM number to determine the count received on each machine
    inline int host_id() { return host_id_; }

    // Clean up the socket after use or if something goes wrong
    void Close()
    {
        io_service_.post([this] { socket_.close(); });
    }

private:
    // Functions that will be called in descending order when sending/receiving data
    void WriteCommand();
    void ReadHeader();
    void ReadResult();
    void ReadLineCount();

    // Meta info
    boost::asio::io_service& io_service_;
    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::resolver::iterator endpoint_;    
    std::string command_;
    int host_id_;
    
    // First data to be received which indicates the size of the result to be sent next
    std::array<char, HEADER_SIZE+1> header_;
    int result_size_;
    
    // The result is a vector, which is resized based on the previous packet
    std::vector<char> result_;

    // This packet contains the line count of the resulting command and is sent after the result
    std::array<char, HEADER_SIZE> count_;
    int line_count_;
};