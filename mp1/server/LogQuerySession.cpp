#include "LogQuerySession.h"

using boost::asio::buffer;
using boost::system::error_code;

// Read a command from the client and attempt to execute it
void LogQuerySession::ReadCommand()
{
    auto self = shared_from_this();
    // Read the grep command supplied by the client and pass it on for execution
    socket_.async_read_some(buffer(command_),
        [this, self](error_code error, size_t bytes_transferred)
        {
            if (!error)
            {
                stripped_command_ = std::string(command_.data(), bytes_transferred);
                ExecuteCommand(stripped_command_);
            }
        });
}

// Run the command and extract the pipe output
void LogQuerySession::ExecuteCommand(const std::string& command)
{   
    std::cout << "Command: " << command << '\n';
    // array used to read stream increments and append it to the result_ string
    std::array<char, STREAM_INCREMENT> stream_block;
    // Execute and read the result of running the supplied command   
    FILE* pipe = popen(command.c_str(), "r");
    while (fgets(stream_block.data(), STREAM_INCREMENT, pipe) != NULL)
    {
        result_ += stream_block.data();
    }
    pclose(pipe);

    WriteOutputSize();
}

// Sends the size of the command result as a header to notify the client of the size of the incoming result
void LogQuerySession::WriteOutputSize()
{
    auto self = shared_from_this();
    EncodeHeader(result_.size());
    boost::asio::async_write(socket_, buffer(header_),
        [this, self](error_code error, size_t bytes_transferred)
        {
            if (!error)
            {
                // Sends the result data after the user receives the size through the header
                WriteOutputBody();
            }
        });
}

// Writes the result to the client
void LogQuerySession::WriteOutputBody()
{
    auto self = shared_from_this();
    boost::asio::async_write(socket_, buffer(result_),
        [this, self](error_code error, size_t bytes_transferred)
        {
            if (!error)
            {
                // Continue on to send the line count after successfully writing all the result data
                WriteOutputCount();
            }
        });
}

// Writes the number of lines in the output
void LogQuerySession::WriteOutputCount()
{
    // array used to read stream increments and append it to the result_ string
    std::array<char, STREAM_INCREMENT> stream_block;
    std::string line_count;
    // Modify the grep command to display the line count instead   
    FILE* pipe = popen((stripped_command_ + " -c").c_str(), "r");
    while (fgets(stream_block.data(), STREAM_INCREMENT, pipe) != nullptr)
    {
        line_count += stream_block.data();
    }
    pclose(pipe);
    auto lines = EncodeLineCount(line_count);
    auto self = shared_from_this();
    boost::asio::async_write(socket_, buffer(lines),
        [this, self](error_code error, size_t bytes_transferred)
        {
            if (!error)
            {
                // Continue on to receive further commands
                ReadCommand();
            }
        });
}
  
// Create the header to represent the number of bytes in the string
void LogQuerySession::EncodeHeader(int result_size)
{
    // Convert the size of the result into a string of known size
    char header[HEADER_SIZE + 1] = "";
    std::sprintf(header, ("%" + std::to_string(HEADER_SIZE) + "d").c_str(), result_size);
    std::memcpy(header_.data(), header, HEADER_SIZE);
}

// Create an array to represent the number of lines
std::array<char, HEADER_SIZE> LogQuerySession::EncodeLineCount(const std::string& line_count)
{
    char count[HEADER_SIZE + 1] = "";
    std::sprintf(count, ("%" + std::to_string(HEADER_SIZE) + "d").c_str(), std::atoi(line_count.c_str()));
    std::array<char, HEADER_SIZE> encoded_count;
    std::memcpy(encoded_count.data(), count, HEADER_SIZE);
    return encoded_count;
}