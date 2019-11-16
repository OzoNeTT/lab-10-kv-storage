#pragma once

#include <boost/filesystem.hpp>
#include <random>
#include <iostream>
#include <thread>
#include <boost/program_options.hpp>
#include <boost/log/trivial.hpp>
#include <boost/program_options/parsers.hpp>
#include "PicoSHA2/picosha2.h"
#include "Globals.h"


int programArguments(int argc, char **argv)
{

    static const std::string OUTPUT_DEFAULT = "dbcs-source";

    boost::program_options::positional_options_description positionalArgs;
    positionalArgs.add("input", -1);

    boost::program_options::options_description visibleOptions("Available options");
    visibleOptions.add_options()
            ("log-level",
             boost::program_options::value<std::string>(&Globals::logLevel)->default_value("error"),
             "debug, info, warning or error level")
            ("thread-count",
             boost::program_options::value<size_t>(&Globals::threadAmount)->default_value(std::thread::hardware_concurrency()),
             "Threads amount")
            ("output",
             boost::program_options::value<std::string>(&Globals::output)->default_value(OUTPUT_DEFAULT),
             "Output path")
            ("help", "Prints help message");

    boost::program_options::options_description hiddenOptions("Hidden options");
    hiddenOptions.add_options()
            ("write-only",
             "Create random db (using input path)")
            ("input",
             boost::program_options::value<std::string>(&Globals::input),
             "Create random db (using input path)");

    boost::program_options::options_description allOptions;
    allOptions.add(visibleOptions).add(hiddenOptions);

    boost::program_options::variables_map variablesMap;
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv)
                           .options(allOptions).positional(positionalArgs).run(),
                   variablesMap);
    boost::program_options::notify(variablesMap);

    if (variablesMap.count("input") && Globals::output == OUTPUT_DEFAULT) {
        Globals::output = "dbcs-" + Globals::input;
    }
    if (variablesMap.count("write-only")) {
        Globals::writeOnly = true;
    }

    if (variablesMap.count("help")) {
        std::cout << visibleOptions << "\n";
        return 1;
    }

    return 0;
}

std::string createRandomString(size_t length)
{
    static const std::string charecters = "1234567890_qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<size_t> random{0, charecters.size() - 1};

    std::string result;
    for (size_t i = 0; i < length; i++) {
        result += charecters[random(generator)];
    }

    return result;
}

void copyDirectory(const boost::filesystem::path &src, const boost::filesystem::path &dst)
{
    if (boost::filesystem::exists(dst)) {
        throw std::runtime_error(dst.generic_string() + " exists");
    }

    if (boost::filesystem::is_directory(src)) {
        boost::filesystem::create_directories(dst);
        for (boost::filesystem::directory_entry &item : boost::filesystem::directory_iterator(src)) {
            copyDirectory(item.path(), dst / item.path().filename());
        }

    } else if (boost::filesystem::is_regular_file(src)) {
        boost::filesystem::copy(src, dst);

    } else {
        throw std::runtime_error(dst.generic_string() + " not dir or file");
    }
}

void removeDirectoryIfExists(const boost::filesystem::path &path)
{
    if (boost::filesystem::exists(path)) {
        boost::filesystem::remove_all(path);
        BOOST_LOG_TRIVIAL(info) << "Removed existing db: " << path;
    }
}
