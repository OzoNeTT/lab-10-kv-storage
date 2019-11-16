#pragma once

#include <string>
#include <random>
#include <utility>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/unordered_map.hpp>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <PicoSHA2/picosha2.h>

#include "Random.h"

class DataBase
{
    std::string path_;
    std::unique_ptr<rocksdb::DB> db_;
public:

    std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> randomFillFamilies();
    void randomFillRows(const std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> &container);
    explicit DataBase(std::string path): path_(std::move(path)){}
    std::vector<rocksdb::ColumnFamilyDescriptor> getFamilyDescriptorList();
    std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> open(const std::vector<rocksdb::ColumnFamilyDescriptor> &descriptors);
    boost::unordered_map<std::string, std::string> getRows(rocksdb::ColumnFamilyHandle *family);
    void hashRows(rocksdb::ColumnFamilyHandle *family,
                  const boost::unordered_map<std::string, std::string>::const_iterator &begin,
                  const boost::unordered_map<std::string, std::string>::const_iterator &end);
    void create();
    void randomFill();
};

std::vector<rocksdb::ColumnFamilyDescriptor> DataBase::getFamilyDescriptorList()
{
    using namespace rocksdb;
    Options options;
    std::vector<std::string> families;
    Status status = DB::ListColumnFamilies(DBOptions(),
                                           path_,
                                           &families);
    assert(status.ok());
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const std::string &familyName: families) {
        descriptors.emplace_back(familyName,
                                 ColumnFamilyOptions{});
    }
    BOOST_LOG_TRIVIAL(debug) << "Got families descriptors";
    return descriptors;
}

std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> DataBase::open(const std::vector<rocksdb::ColumnFamilyDescriptor> &descriptors)
{
    using namespace rocksdb;
    std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handlers;
    std::vector<ColumnFamilyHandle *> pureHandlers;
    DB *dbRawPointer;
    Status status = DB::Open(DBOptions{},
                             path_,
                             descriptors,
                             &pureHandlers,
                             &dbRawPointer);
    assert(status.ok());
    db_.reset(dbRawPointer);
    for (ColumnFamilyHandle *pointer : pureHandlers) {
        BOOST_LOG_TRIVIAL(debug) << "Got family: " << pointer->GetName();
        handlers.emplace_back(pointer);
    }
    return handlers;
}

boost::unordered_map<std::string, std::string> DataBase::getRows(rocksdb::ColumnFamilyHandle *family)
{
    using namespace rocksdb;
    BOOST_LOG_TRIVIAL(debug) << "Rewrite family: " << family->GetName();
    boost::unordered_map<std::string, std::string> toWrite;
    std::unique_ptr<Iterator> it{db_->NewIterator(ReadOptions{}, family)};
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        toWrite[key] = value;

        BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
    }
    if (!it->status().ok()) {
        BOOST_LOG_TRIVIAL(error) << it->status().ToString();
    }
    return toWrite;
}

void DataBase::hashRows(rocksdb::ColumnFamilyHandle *family,const boost::unordered_map<std::string, std::string>::const_iterator &begin,const boost::unordered_map<std::string, std::string>::const_iterator &end)
{
    using namespace rocksdb;
    for (auto it = begin; it != end; ++it) {
        auto &&[key, value] = *it;
        std::string toHash = key;
        toHash += ":" + value;
        std::string hash = picosha2::hash256_hex_string(toHash);
        Status status = db_->Put(WriteOptions(),
                                 family,
                                 key,
                                 hash);
        assert(status.ok());
        BOOST_LOG_TRIVIAL(info) << "Hashed from '" << family->GetName() << "': " << key;
        BOOST_LOG_TRIVIAL(debug) << "Put: " << key << " : " << hash;
    }
}

void DataBase::create()
{
    using namespace rocksdb;
    removeDirectoryIfExists(path_);
    Options options;
    options.create_if_missing = true;
    DB *dbRawPointer;
    Status status = DB::Open(options, path_, &dbRawPointer);
    assert(status.ok());
    db_.reset(dbRawPointer);
}

void DataBase::randomFill()
{
    auto families = randomFillFamilies();
    randomFillRows(families);
}

std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> DataBase::randomFillFamilies()
{
    using namespace rocksdb;
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<size_t> randomFamilyAmount{1, 5};
    size_t familyAmount = randomFamilyAmount(generator);
    std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> families{};        // RAII wrapper
    for (size_t i = 0; i < familyAmount; i++) {
        static const size_t FAMILY_NAME_LENGTH = 5;
        ColumnFamilyHandle *familyRawPointer;
        std::string familyName = createRandomString(FAMILY_NAME_LENGTH);
        Status status = db_->CreateColumnFamily(ColumnFamilyOptions(),
                                                createRandomString(FAMILY_NAME_LENGTH),
                                                &familyRawPointer);
        assert(status.ok());
        families.emplace_back(familyRawPointer);
        BOOST_LOG_TRIVIAL(info) << "Create family: " << familyName;
    }

    return families;
}

void DataBase::randomFillRows(const std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>> &container)
{
    using namespace rocksdb;
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<size_t> randomRowAmount{5, 25};
    static const size_t key_l = 5;
    static const size_t value_l = 10;
    size_t defaultRowAmount = randomRowAmount(generator);

    BOOST_LOG_TRIVIAL(debug) << "Fill family: default";
    for (size_t i = 0; i < defaultRowAmount; i++) {
        std::string key = createRandomString(key_l);
        std::string value = createRandomString(value_l);
        Status status = db_->Put(WriteOptions(),
                                 key,
                                 value);
        assert(status.ok());
        BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
    }

    for (const std::unique_ptr<ColumnFamilyHandle> &family : container) {
        BOOST_LOG_TRIVIAL(debug) << "Fill family: " << family->GetName();
        size_t rowAmount = randomRowAmount(generator);
        for (size_t i = 0; i < rowAmount; i++) {
            std::string key = createRandomString(key_l);
            std::string value = createRandomString(value_l);

            Status status = db_->Put(WriteOptions(),
                                     family.get(),
                                     key,
                                     value);
            assert(status.ok());
            BOOST_LOG_TRIVIAL(debug) << key << " : " << value;
        }
    }
}