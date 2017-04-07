// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <vector>

#include "mongo/bson/bson.h"

#include "io/output/outputformat_base.hpp"

namespace husky {
namespace io {

class MongoDBOutputFormat final : public OutputFormatBase {
   public:
    MongoDBOutputFormat();
    ~MongoDBOutputFormat();
    void set_auth(const std::string& username, const std::string& password);
    void set_ns(const std::string& database, const std::string& collection);
    void set_server(std::string server);
    virtual bool is_setup() const;

    bool commit(const std::string& doc);
    bool commit(const mongo::BSONObj& obj);
    void flush_all();

   protected:
    bool need_auth_ = false;
    std::string collection_;
    std::string database_;
    std::string error_msg_;
    std::string ns_;
    std::string password_;
    std::string server_;
    std::string username_;
    std::vector<mongo::BSONObj> records_vector_;
};

}  // namespace io
}  // namespace husky
