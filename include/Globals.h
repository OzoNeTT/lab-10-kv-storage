#pragma once

#include <string>

struct Globals
{
    static std::string logLevel;
    static size_t threadAmount;
    static std::string output;
    static std::string input;
    static bool writeOnly;
};

std::string Globals::logLevel{};
size_t Globals::threadAmount{0};
std::string Globals::output{};
std::string Globals::input{};
bool Globals::writeOnly{false};