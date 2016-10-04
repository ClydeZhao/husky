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

#include <string>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

class Vertex {
   public:
    using KeyT = int;
    Vertex() = default;
    explicit Vertex(const KeyT id) : id_(id) {}
    const KeyT id() const { return id_; }

    // serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.id_ << v.neighbors_;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.id_ >> v.neighbors_;
        return stream;
    }

    KeyT id_;
    std::vector<KeyT> neighbors_;
};

template <typename MsgT>
struct MinCombiner {
    static void combine(MsgT& val, MsgT const& inc) {
        if (inc < val) {
            val = inc;
        }
    }
};

void pr_cc() {
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    // create vertex object list and attribute list for pagerank value
    auto& vertex_list = husky::ObjListFactory::create_objlist<Vertex>();
    auto& pr_list = vertex_list.create_attrlist<float>("pr");
    auto& cid_list = vertex_list.create_attrlist<int>("cid");
    auto parser = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        it++;
        Vertex v(id);
        while (it != tok.end()) {
            v.neighbors_.push_back(stoi(*it++));
        }
        auto idx = vertex_list.add_object(std::move(v));
        pr_list[idx] = 0.15;
        cid_list[idx] = id;
    };
    husky::load(infmt, parser);
    husky::globalize(vertex_list);

    // iterative PageRank computation
    auto& prch =
        husky::ChannelFactory::create_push_combined_channel<float, husky::SumCombiner<float>>(vertex_list, vertex_list);
    int numIters = stoi(husky::Context::get_param("iters"));
    for (int iter = 0; iter < numIters; ++iter) {
        husky::list_execute(vertex_list, [&prch, &pr_list, iter](Vertex& v) {
            // auto& pr = pr_list.get(v);
            if (v.neighbors_.size() == 0)
                return;
            if (iter > 0)
                pr_list[v] = 0.85 * prch.get(v) + 0.15;

            float send_pr = pr_list[v] / v.neighbors_.size();
            for (auto& nb : v.neighbors_) {
                prch.push(send_pr, nb);
            }
        });
    }

    // connected component computation
    husky::lib::Aggregator<int> not_finished;
    not_finished.update(1);
    not_finished.to_reset_each_iter();

    auto& ac = husky::lib::AggregatorFactory::get_channel();
    auto& cidch = husky::ChannelFactory::create_push_combined_channel<int, MinCombiner<int>>(vertex_list, vertex_list);

    husky::list_execute(vertex_list, {}, {&ac}, [&cidch, &cid_list, &not_finished](Vertex& v) {
        int& cid = cid_list[v];
        int min = cid;
        for (auto nb : v.neighbors_) {
            if (nb < min) {
                min = nb;
            }
        }
        cid = min;
        for (auto nb : v.neighbors_) {
            cidch.push(nb, min);
        }
    });

    while (not_finished.get_value()) {
        husky::list_execute(vertex_list, {}, {&ac}, [&cidch, &cid_list, &not_finished](Vertex& v) {
            int u = cidch.get(v);
            if (u < cid_list[v]) {
                cid_list[v] = u;
                not_finished.update(1);
                for (auto nb : v.neighbors_) {
                    cidch.push(nb, u);
                }
            }
        });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("iters");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pr_cc);
        return 0;
    }
}
