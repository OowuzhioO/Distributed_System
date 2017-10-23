#include "LogQueryClient.h"

using boost::asio::buffer;
using boost::asio::ip::tcp;
using boost::system::error_code;
using std::atoi;
using std::cout;
using std::string;
using std::to_string;

namespace
{
    // Hostnames for the VMs are in the format: fa17-cs425-g48-XX.cs.illinois.edu where XX is distinct
    string GenerateHostname(int host_id)
    {
        string hostname = "fa17-cs425-g48-";
        // Single digit hosts have a '0' before the host_id in the hostname
        hostname += (host_id < 10) ? '0' + to_string(host_id) : to_string(host_id);
        hostname += ".cs.illinois.edu";
        return hostname;
    }
}

// Constructs and begins the connection and data transfer process
LogQueryClient::LogQueryClient(boost::asio::io_service& io_service, const string& command, int host_id)
    : io_service_(io_service)
    , socket_(io_service)
    , command_(command)
    , host_id_(host_id)
    , line_count_(0)
{
    // Connect the VM on port 1867 and begin writing the grep command
    tcp::resolver resolver(io_service);
    endpoint_ = resolver.resolve({GenerateHostname(host_id), "1867"});
    boost::asio::async_connect(socket_, endpoint_,
        [this](error_code error, tcp::resolver::iterator)
        {
            if (!error)
            {
                WriteCommand();
            }
        });
}

// Write the (grep) command to be executed on the servers
void LogQueryClient::WriteCommand()
{
    // Look for a size reply after writing the command
    boost::asio::async_write(socket_, buffer(command_),
        [this](error_code error, size_t bytes)
        {
            if (!error)
            {
                ReadHeader();
            }
            else
            {
                socket_.close();
            }
        });
}

// After sending the command, look for size data to prepare the result retrieval
void LogQueryClient::ReadHeader()
{
    // Read the number of bytes in the output then read the actual output
    boost::asio::async_read(socket_, buffer(header_, HEADER_SIZE),
        [this](error_code error, size_t bytes)
        {
            // Stop receiving if there's nothing to receive
            if (!error && atoi(header_.data()) > 0)
            {
                result_size_ = atoi(header_.data());
                ReadResult();
            }
            else
            {
                socket_.close();
            }
        });
}

// Now read all of the result data into a result_ string
void LogQueryClient::ReadResult()
{
    // Resize to accomodate the entire output
    result_.resize(result_size_);
    boost::asio::async_read(socket_, buffer(result_, result_size_),
        [this](error_code error, size_t bytes)
        {
            if (!error)
            {
                // write the grep output to a file
                std::ofstream file;
                file.open("log_query" + to_string(host_id_) + ".txt");
                file << result_.data();
                file.close();

                ReadLineCount();
            }
            else
            {
                socket_.close();
            }
        });
}

// Once all of the result data has been received, start looking for the line count
void LogQueryClient::ReadLineCount()
{
    //Then read the number of lines in the output
    socket_.async_read_some(buffer(count_, HEADER_SIZE),
        [this](error_code error, size_t bytes)
        {
            if (!error)
            {
                line_count_ = atoi(count_.data());
            }
            else
            {
                socket_.close();
            }
        });
}