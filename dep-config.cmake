# Copyright 2016 Husky Team
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


### ZeroMQ ###

set(ZMQ_FIND_REQUIRED true)
find_path(ZMQ_INCLUDE_DIR NAMES zmq.hpp)
find_library(ZMQ_LIBRARY NAMES zmq)
if(ZMQ_INCLUDE_DIR AND ZMQ_LIBRARY)
    set(ZMQ_FOUND true)
endif(ZMQ_INCLUDE_DIR AND ZMQ_LIBRARY)
if(ZMQ_FOUND)
    set(DEP_FOUND true)
    if(NOT ZMQ_FIND_QUIETLY)
        message (STATUS "Found ZeroMQ:")
        message (STATUS "  (Headers)       ${ZMQ_INCLUDE_DIR}")
        message (STATUS "  (Library)       ${ZMQ_LIBRARY}")
    endif(NOT ZMQ_FIND_QUIETLY)
else(ZMQ_FOUND)
    set(DEP_FOUND false)
    if(ZMQ_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find ZeroMQ")
    endif(ZMQ_FIND_REQUIRED)
endif(ZMQ_FOUND)

### TCMalloc ###
set(TCMALLOC_FIND_REQUIRED true)
find_library(TCMALLOC_LIBRARY NAMES tcmalloc)
if(TCMALLOC_LIBRARY)
    set(TCMALLOC_FOUND true)
endif(TCMALLOC_LIBRARY)
if(TCMALLOC_FOUND)
    set(DEP_FOUND true)
    if(NOT TCMALLOC_FIND_QUIETLY)
        message (STATUS "Found TCMalloc:")
        message (STATUS "  (Library)       ${TCMALLOC_LIBRARY}")
    endif(NOT TCMALLOC_FIND_QUIETLY)
else(TCMALLOC_FOUND)
    set(DEP_FOUND false)
    if(TCMALLOC_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find TCMalloc")
    endif(TCMALLOC_FIND_REQUIRED)
endif(TCMALLOC_FOUND)

### LibHDFS3 ###

find_path(LIBHDFS3_INCLUDE_DIR NAMES hdfs/hdfs.h)
find_library(LIBHDFS3_LIBRARY NAMES hdfs3)
if(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
    set(LIBHDFS3_FOUND true)
endif(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
if(LIBHDFS3_FOUND)
    if(NOT LIBHDFS3_FIND_QUIETLY)
        message (STATUS "Found libhdfs3:")
        message (STATUS "  (Headers)       ${LIBHDFS3_INCLUDE_DIR}")
        message (STATUS "  (Library)       ${LIBHDFS3_LIBRARY}")
    endif(NOT LIBHDFS3_FIND_QUIETLY)
else(LIBHDFS3_FOUND)
    message(STATUS "Could NOT find libhdfs3")
endif(LIBHDFS3_FOUND)
if(WITHOUT_HDFS)
    unset(LIBHDFS3_FOUND)
    message(STATUS "Not using libhdfs3 due to WITHOUT_HDFS option")
endif(WITHOUT_HDFS)

### MongoDB ###

find_path(MONGOCLIENT_INCLUDE_DIR NAMES mongo)
find_library(MONGOCLIENT_LIBRARY NAMES mongoclient)
if (MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
    set(MONGOCLIENT_FOUND true)
endif(MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
if (MONGOCLIENT_FOUND)
    message (STATUS "Found MongoClient:")
    message (STATUS "  (Headers)       ${MONGOCLIENT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${MONGOCLIENT_LIBRARY}")
else(MONGOCLIENT_FOUND)
    message (STATUS "Could NOT find MongoClient")
endif(MONGOCLIENT_FOUND)
if(WITHOUT_MONGODB)
    unset(MONGOCLIENT_FOUND)
    message(STATUS "Not using MongoClient due to WITHOUT_MONGODB option")
endif(WITHOUT_MONGODB)

### RT ###

find_library(RT_LIBRARY NAMES rt)
if(RT_LIBRARY)
    set(RT_FOUND true)
else(RT_LIBRARY)
    set(RT_FOUND false)
endif(RT_LIBRARY)
if (RT_FOUND)
    message (STATUS "Found RT:")
    message (STATUS "  (Library)       ${RT_LIBRARY}")
else(RT_FOUND)
    message (STATUS "Could NOT find RT")
endif(RT_FOUND)

### Thrift ###

find_path(THRIFT_INCLUDE_DIR NAMES thrift)
find_library(THRIFT_LIBRARY NAMES thrift)
if (THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
    set(THRIFT_FOUND true)
endif(THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
if (THRIFT_FOUND)
    message (STATUS "Found Thrift:")
    message (STATUS "  (Headers)       ${THRIFT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${THRIFT_LIBRARY}")
else(THRIFT_FOUND)
    message (STATUS "Could NOT find Thrift")
endif(THRIFT_FOUND)
if(WITHOUT_THRIFT)
    unset(THRIFT_FOUND)
    message(STATUS "Not using Thrift due to WITHOUT_THRIFT option")
endif(WITHOUT_THRIFT)

### ORC ###

#NAMES liblz4.a liborc.a libprotobuf.a libsnappy.a libz.a 
#NAMES ColumnPrinter.hh Int128.hh MemoryPool.hh orc-config.hh OrcFile.hh Reader.hh Type.hh  Vector.hh
find_path(ORC_INCLUDE_DIR NAMES ColumnPrinter.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES Int128.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES orc-config.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES OrcFile.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES Reader.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES Type.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_path(ORC_INCLUDE_DIR NAMES Vector.hh PATHS /data/yidi/T1/husky/orc/orc_include/orc/) 
find_library(ORC_L0 NAMES protobuf  PATHS /data/yidi/T1/husky/orc/lib/ NO_DEFAULT_PATH)
find_library(ORC_L1 NAMES z  PATHS /data/yidi/T1/husky/orc/lib/ NO_DEFAULT_PATH)
find_library(ORC_L2 NAMES lz4  PATHS /data/yidi/T1/husky/orc/lib/ NO_DEFAULT_PATH)
find_library(ORC_L3 NAMES snappy  PATHS /data/yidi/T1/husky/orc/lib NO_DEFAULT_PATH)
find_library(ORC_L4 NAMES orc  PATHS /data/yidi/T1/husky/orc/lib/ NO_DEFAULT_PATH)

if (ORC_INCLUDE_DIR AND ORC_L1 AND ORC_L0 AND ORC_L2 AND ORC_L3 AND ORC_L4)
    set(ORC_FOUND true)
endif (ORC_INCLUDE_DIR AND ORC_L1 AND ORC_L0 AND ORC_L2 AND ORC_L3 AND ORC_L4)
if (ORC_FOUND)
    message (STATUS "Found ORC:")
    message (STATUS "  (Headers)       ${ORC_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${ORC_L0}")
    message (STATUS "  (Library)       ${ORC_L1}")
    message (STATUS "  (Library)       ${ORC_L2}")
    message (STATUS "  (Library)       ${ORC_L3}")
    message (STATUS "  (Library)       ${ORC_L4}")
else(ORC_FOUND)
    message (STATUS "Could NOT find ORC")
endif(ORC_FOUND)
if(WITHOUT_ORC)
    unset(ORC_FOUND)
    message(STATUS "Not using ORC due to WITHOUT_MONGODB option")
endif(WITHOUT_ORC)
