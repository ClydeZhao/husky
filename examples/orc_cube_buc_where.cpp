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

#include <climits>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/hdfs_manager.hpp"
#include "io/input/orc_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

typedef boost::tokenizer<boost::char_separator<char>> Tokenizer;
typedef std::map<int, int> DimMap;
typedef std::pair<int, int> Pair;
typedef std::pair<std::string, std::string> Filter;
typedef std::map<int, Filter> FilterMap;
typedef std::vector<int> Attribute;
typedef std::vector<std::string> Tuple;
typedef std::vector<Tuple> TupleVector;
typedef TupleVector::iterator TVIterator;

thread_local std::string ghost;
thread_local std::string gport;
thread_local std::string ghdfs_dest;
thread_local int gpart_factor;

using husky::PushCombinedChannel;
using husky::lib::Aggregator;

class Group {
   public:
    using KeyT = std::string;

    Group() = default;
    explicit Group(const KeyT& t) : key(t) {}
    // explicit Group(Tuple&& t) : key(std::move(t)) {}

    const KeyT& id() { return key; }
    KeyT key;
};

struct PairSumCombiner {
    static void combine(Pair& val, Pair const& inc) {
        val.first += inc.first;
        val.second += inc.second;
    }
};

class TreeNode {
   public:
    TreeNode() = default;
    explicit TreeNode(Attribute&& key) : key_(std::move(key)) { visit = false; }

    explicit TreeNode(const Attribute& key) : key_(key) { visit = false; }

    ~TreeNode() = default;

    bool visit;

    const Attribute& Key() { return key_; }

    std::vector<std::shared_ptr<TreeNode>>& Children() { return children_; }

    void add_child(std::shared_ptr<TreeNode> child) { children_.push_back(child); }

   private:
    Attribute key_;
    std::vector<std::shared_ptr<TreeNode>> children_;
};

bool is_parent(std::shared_ptr<TreeNode> parent, std::shared_ptr<TreeNode> child) {
    auto child_key = child->Key();
    for (auto& col : parent->Key()) {
        if (std::find(child_key.begin(), child_key.end(), col) == child_key.end()) {
            return false;
        }
    }
    return true;
}

std::string print_key(const Attribute& key) {
    std::string out;
    for (auto& i : key) {
        out = out + std::to_string(i) + " ";
    }
    return out;
}

void measure(const Tuple& key_value, const Attribute& group_attributes, const Attribute& select,
             const Attribute& key_attributes, DimMap& key_dim_map, DimMap& msg_dim_map, const int uid_dim,
             TVIterator begin, TVIterator end, PushCombinedChannel<Pair, Group, PairSumCombiner>& post_ch,
             Aggregator<int>& agg) {
    int count = end - begin;
    std::sort(begin, end, [uid_dim](const Tuple& a, const Tuple& b) { return a[uid_dim] < b[uid_dim]; });
    int unique = 1;
    for (TVIterator it = begin; it != end; ++it) {
        TVIterator next_it = it + 1;
        if (next_it != end && (*it)[uid_dim] != (*next_it)[uid_dim]) {
            ++unique;
        }
    }

    // Output
    std::string out;
    for (auto& attr : select) {
        // If attribute is in key,
        //     output key value.
        // Else,
        //     If attribute is in group,
        //         output attribute in the tuple
        //     Else,
        //         output *
        if (std::find(key_attributes.begin(), key_attributes.end(), attr) != key_attributes.end()) {
            out = out + key_value[key_dim_map[attr]] + "\t";
        } else {
            if (std::find(group_attributes.begin(), group_attributes.end(), attr) != group_attributes.end()) {
                out = out + (*begin)[msg_dim_map[attr]] + "\t";
            } else {
                out += "*\t";
            }
        }
    }

    if (gpart_factor == 1) {
        out = out + std::to_string(count) + "\t" + std::to_string(unique) + "\n";
        agg.update(1);
        std::string hdfs_dest = ghdfs_dest + "/" + key_value.back();
        husky::io::HDFS::Write(ghost, gport, out, hdfs_dest, husky::Context::get_global_tid());
    } else {
        out += key_value.back();
        post_ch.push(Pair(count, unique), out);
    }
}

int next_partition_dim(const Attribute& parent_key, const Attribute& child_key, DimMap& dim_map) {
    for (auto& attr : child_key) {
        if (std::find(parent_key.begin(), parent_key.end(), attr) == parent_key.end()) {
            return dim_map[attr];
        }
    }
    // error
    return -1;
}

// Parition the table according to value at the 'dim'-th column
void partition(TVIterator begin, TVIterator end, const int dim, std::vector<int>& out_partition_result) {
    std::sort(begin, end, [dim](const Tuple& a, const Tuple& b) { return a[dim] < b[dim]; });
    int i = 0;
    // Store the size of each partition
    out_partition_result.resize(1);
    TVIterator next_tuple;
    for (TVIterator it = begin; it != end; ++it) {
        out_partition_result[i]++;
        next_tuple = it + 1;
        // If value of next row differs at the dim-th column,
        //     partition the table
        if (next_tuple != end && (*it)[dim] != (*next_tuple)[dim]) {
            ++i;
            out_partition_result.resize(i + 1);
        }
    }
}

void BUC(std::shared_ptr<TreeNode> cur_node, TupleVector& table, const Tuple& key_value, const Attribute& select,
         const Attribute& key_attributes, DimMap& key_dim_map, DimMap& msg_dim_map, const int uid_dim, const int dim,
         const int table_size, TVIterator begin, TVIterator end,
         PushCombinedChannel<Pair, Group, PairSumCombiner>& post_ch, Aggregator<int>& agg) {
    // Measure current group
    measure(key_value, cur_node->Key(), select, key_attributes, key_dim_map, msg_dim_map, uid_dim, begin, end, post_ch,
            agg);

    // Process children if it is not visited
    for (auto& child : cur_node->Children()) {
        // Partition table by next column
        int next_dim = next_partition_dim(cur_node->Key(), child->Key(), msg_dim_map);
        // TODO(Ruihao): handle error if next_dim == -1
        std::vector<int> next_partition_result = {};
        partition(begin, end, next_dim, next_partition_result);
        // Perform BUC on each partition
        TVIterator k = begin;
        for (int i = 0; i < next_partition_result.size(); ++i) {
            int count = next_partition_result[i];
            BUC(child, table, key_value, select, key_attributes, key_dim_map, msg_dim_map, uid_dim, next_dim,
                table_size, k, k + count, post_ch, agg);
            k += count;
        }
    }
}

bool is_operator(const char& c) { return (c == '<' or c == '>' or c == '='); }

void parse_group_set(const std::string& group_filter, const Tokenizer& schema_tok,
                     std::vector<std::shared_ptr<TreeNode>>& out_roots, std::vector<FilterMap>& out_filters) {
    boost::char_separator<char> vbar_sep("|");
    boost::char_separator<char> comma_sep(",");
    boost::char_separator<char> colon_sep(":");

    Tokenizer group_filter_tok(group_filter, vbar_sep);
    Tokenizer::iterator gf_it = group_filter_tok.begin();

    /**
     * Process group sets
     */
    Tokenizer group_set_tok(*gf_it, colon_sep);
    std::shared_ptr<TreeNode> root;
    int min_lv = INT_MAX;
    int max_lv = INT_MIN;

    std::unordered_map<int, std::vector<std::shared_ptr<TreeNode>>> tree_map;
    size_t group_set_size = std::distance(group_set_tok.begin(), group_set_tok.end());
    for (auto& group : group_set_tok) {
        // Encode and construct key of the node
        Tokenizer column_tok(group, comma_sep);
        Attribute tree_key = {};
        for (auto column : column_tok) {
            auto it = std::find(schema_tok.begin(), schema_tok.end(), column);
            if (it != schema_tok.end()) {
                tree_key.push_back(std::distance(schema_tok.begin(), it));
            } else {
                throw husky::base::HuskyException("Invalid schema or group sets");
            }
        }
        int level = tree_key.size();
        std::shared_ptr<TreeNode> node(new TreeNode(std::move(tree_key)));
        tree_map[level].push_back(node);
        if (level < min_lv) {
            min_lv = level;
            root = node;
        }
        if (level > max_lv) {
            max_lv = level;
        }
    }
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Min level: " + std::to_string(min_lv) + "\tMax level: " + std::to_string(max_lv));
    }

    // Build group set lattice
    bool has_parent = false;
    for (int i = min_lv; i < max_lv; ++i) {
        if (tree_map[i].empty()) {
            throw husky::base::HuskyException("Level " + std::to_string(i) + " is empty");
        }
        for (auto& next_tn : tree_map[i + 1]) {
            for (auto& tn : tree_map[i]) {
                if (is_parent(tn, next_tn)) {
                    tn->add_child(next_tn);
                    has_parent = true;
                }
            }
            if (!has_parent) {
                throw husky::base::HuskyException("Cannot find the parent of " + print_key(next_tn->Key()));
            }
            has_parent = false;
        }
    }
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Finished constructing lattice.");
    }

    // Construct BUC processing tree
    std::shared_ptr<TreeNode> buc_root(new TreeNode(root->Key()));
    std::stack<std::shared_ptr<TreeNode>> tmp_stack;
    std::stack<std::shared_ptr<TreeNode>> buc_stack;
    tmp_stack.push(root);
    buc_stack.push(buc_root);
    while (!tmp_stack.empty()) {
        std::shared_ptr<TreeNode> cur_node = tmp_stack.top();
        tmp_stack.pop();
        std::shared_ptr<TreeNode> cur_buc_node = buc_stack.top();
        buc_stack.pop();
        cur_node->visit = true;
        for (auto& child : cur_node->Children()) {
            if (!child->visit) {
                tmp_stack.push(child);
                std::shared_ptr<TreeNode> new_buc_node(new TreeNode(child->Key()));
                cur_buc_node->add_child(new_buc_node);
                buc_stack.push(new_buc_node);
            }
        }
    }
    out_roots.push_back(buc_root);
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Finished constructing buc processing tree.");
    }

    /**
     * Process WHERE
     * Format AttrOperatorValue, e.g., fuid<>1:fcard_type='123'
     */
    FilterMap filter;
    if (std::distance(group_filter_tok.begin(), group_filter_tok.end()) == 2) {
        gf_it++;
        Tokenizer where_tok(*gf_it, colon_sep);
        for (auto& where : where_tok) {
            int pos[2] = {};
            std::string where_str = where;
            for (int i = 0; i < where_str.length(); ++i) {
                if (pos[0] == 0) {
                    if (is_operator(where_str[i]))
                        pos[0] = i;
                } else {
                    if (!is_operator(where_str[i])) {
                        pos[1] = i;
                        break;
                    }
                }
            }
            if (pos[0] == 0 or pos[1] == 0) {
                throw husky::base::HuskyException("Invalid syntax in WHERE");
            }
            std::string attr = where_str.substr(0, pos[0]);
            std::string op = where_str.substr(pos[0], pos[1] - pos[0]);
            std::string value = where_str.substr(pos[1], where_str.length() - pos[1]);

            auto it = std::find(schema_tok.begin(), schema_tok.end(), attr);
            if (it == schema_tok.end()) {
                throw husky::base::HuskyException("Invalid attribute in WHERE");
            }
            int attr_idx = std::distance(schema_tok.begin(), it);
            filter[attr_idx] = Filter(op, value);
        }
    }
    out_filters.push_back(filter);
}

bool pass_filter(const std::string& value, const Filter& filter) {
    if (boost::iequals(filter.second, std::string("null"))) 
        return true;  // Always return true if compare against null.
                      // Consistent to SQL
    
    if(filter.first == "<>")
        return value != filter.second;
    if(filter.first == ">")
        return value > filter.second;
    if(filter.first == "<")
        return value < filter.second;
    if(filter.first == ">=")
        return value >= filter.second;
    if(filter.first == "<=")
        return value <= filter.second;
    if(filter.first == "=")
        return value == filter.second;
}

void print_buc_tree(const std::shared_ptr<TreeNode>& root) {
    husky::base::log_msg(print_key(root->Key()));
    for (auto& child : root->Children()) {
        print_buc_tree(child);
    }
}

void print_filter_map(const FilterMap& fmap) {
    for (auto& kv : fmap) {
        husky::base::log_msg(std::to_string(kv.first) + " " + kv.second.first + " " + kv.second.second);
    }
}

void cube_buc() {
    gpart_factor = std::stoi(husky::Context::get_param("partition_factor"));
    ghost = husky::Context::get_param("hdfs_namenode");
    gport = husky::Context::get_param("hdfs_namenode_port");
    ghdfs_dest = husky::Context::get_param("output");

    /**
     * Format of 'schema' and 'select':
     *     attr1,attr2,attr3,...
     */
    std::string schema_conf = husky::Context::get_param("schema");
    std::string select_conf = husky::Context::get_param("select");

    /**
     * Format of 'group_sets':
     *     {GROUP_SETS_1|WHERE_1}{GROUP_SET_2|WHERE_2}{...}{...}
     *         Format of GROUP_SET:
     *             arrt1,attr2,attr3:attr2,attr3,attr4:...:...
     *         Format of WHERE
     *             arrt1<>value:attr2=value:...:...
     */
    std::string group_conf = husky::Context::get_param("group_sets");

    boost::char_separator<char> comma_sep(",");
    boost::char_separator<char> colon_sep(":");
    boost::char_separator<char> brace_sep("{}");

    Tokenizer schema_tok(schema_conf, comma_sep);
    Tokenizer select_tok(select_conf, comma_sep);
    Tokenizer group_filter(group_conf, brace_sep);

    Attribute select;
    for (auto& s : select_tok) {
        auto it = std::find(schema_tok.begin(), schema_tok.end(), s);
        if (it != schema_tok.end()) {
            select.push_back(std::distance(schema_tok.begin(), it));
        }
        // TODO(Ruihao): Throw expection if input is wrong?
    }

    std::vector<std::shared_ptr<TreeNode>> root_vec;
    std::vector<FilterMap> filter_vec;
    for (auto& item : group_filter) {
        std::string item_str = item;
        parse_group_set(item_str, schema_tok, root_vec, filter_vec);
    }

    int uid_index = -1;
    // TODO(Ruihao): Attribute to count is hard-coded as "fuid"
    auto uid_it = std::find(schema_tok.begin(), schema_tok.end(), "fuid");
    if (uid_it != schema_tok.end()) {
        uid_index = std::distance(schema_tok.begin(), uid_it);
    } else {
        throw husky::base::HuskyException("Cannot find fuid");
    }

    std::vector<Attribute> key_attr_vec;
    std::vector<Attribute> msg_attr_vec;
    std::vector<DimMap> msg_dim_map_vec;
    std::vector<DimMap> key_dim_map_vec;

    for (int i = 0; i < root_vec.size(); ++i) {
        // {key} union {msg} = {select}
        // {key} intersect {msg} = empty
        Attribute key_attributes = root_vec[i]->Key();
        Attribute msg_attributes;
        for (auto& s : select) {
            if (std::find(key_attributes.begin(), key_attributes.end(), s) == key_attributes.end()) {
                msg_attributes.push_back(s);
            }
        }

        // Mapping of attributes in the message table
        // schema_idx -> msg_table_idx
        DimMap msg_dim_map;
        for (int i = 0; i < msg_attributes.size(); ++i) {
            msg_dim_map[msg_attributes[i]] = i;
        }

        // Mapping of attributes in key
        DimMap key_dim_map;
        for (int i = 0; i < key_attributes.size(); ++i) {
            key_dim_map[key_attributes[i]] = i;
        }

        key_attr_vec.push_back(key_attributes);
        msg_attr_vec.push_back(msg_attributes);
        msg_dim_map_vec.push_back(msg_dim_map);
        key_dim_map_vec.push_back(key_dim_map);
    }

    // Load input and emit key\tpid\ti -> uid
    husky::io::ORCInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    auto& buc_list = husky::ObjListFactory::create_objlist<Group>("buc_list");
    auto& buc_ch = husky::ChannelFactory::create_push_channel<Tuple>(infmt, buc_list);
    auto& post_list = husky::ObjListFactory::create_objlist<Group>("post_list");
    auto& post_ch = husky::ChannelFactory::create_push_combined_channel<Pair, PairSumCombiner>(buc_list, post_list);

    Aggregator<int> agg(0, [](int& a, const int& b) { a += b; });
    Aggregator<int> num_tuple;
    agg.to_keep_aggregate();
    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    auto parser = [&](boost::string_ref& chunk) {
        std::vector<bool> to_send(root_vec.size(), true);
        num_tuple.update(1);
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep("\t");
        Tokenizer tok(chunk, sep);
        for (int i = 0; i < root_vec.size(); ++i) {
            auto& filter_map = filter_vec[i];
            auto& key_attributes = key_attr_vec[i];
            auto& msg_attributes = msg_attr_vec[i];
            auto& msg_dim_map = msg_dim_map_vec[i];
            std::string key = "";
            Tuple msg(msg_attributes.size());
            std::string fuid;
            int j = 0;
            for (auto& col : tok) {
                if (filter_map.find(j) != filter_map.end() and !pass_filter(col, filter_map[j])) {
                    to_send[i] = false;
                    break;
                }

                if (std::find(key_attributes.begin(), key_attributes.end(), j) != key_attributes.end()) {
                    key = key + col + "\t";
                } else if (std::find(msg_attributes.begin(), msg_attributes.end(), j) != msg_attributes.end()) {
                    msg[msg_dim_map[j]] = col;
                } else if (j == uid_index) {
                    fuid = col;
                }
                ++j;
            }
            if (to_send[i]) {
                msg.push_back(fuid);
                if (gpart_factor > 1) {
                    int bucket = std::stoi(fuid) % gpart_factor;
                    key = key + "p" + std::to_string(bucket) + "\t" + std::to_string(i);
                }
                buc_ch.push(msg, key);
            }
        }
    };

    husky::load(infmt, parser);
    husky::lib::AggregatorFactory::sync();
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Total num of tuple: " + std::to_string(num_tuple.get_value()));
    }

    // Receive
    husky::list_execute(buc_list, {&buc_ch}, {&post_ch, &agg_ch}, [&](Group& g) {
        auto& msgs = buc_ch.get(g);
        TupleVector table(std::move(const_cast<TupleVector&>(msgs)));
        boost::char_separator<char> sep("\t");
        boost::tokenizer<boost::char_separator<char>> tok(g.id(), sep);
        std::vector<std::string> key_value(tok.begin(), tok.end());

        int filter_idx = std::stoi(key_value.back());
        key_value.pop_back();
        // Remove the hash value
        key_value.pop_back();
        key_value.push_back("w" + std::to_string(filter_idx));

        auto& buc_root = root_vec[filter_idx];
        auto& key_attributes = key_attr_vec[filter_idx];
        auto& key_dim_map = key_dim_map_vec[filter_idx];
        auto& msg_dim_map = msg_dim_map_vec[filter_idx];
        auto& msg_attributes = msg_attr_vec[filter_idx];
        int uid_dim = msg_attributes.size();

        BUC(buc_root, table, key_value, select, key_attributes, key_dim_map, msg_dim_map, uid_dim, 0, table.size(),
            table.begin(), table.end(), post_ch, agg);
    });

    if (gpart_factor > 1) {
        if (husky::Context::get_global_tid() == 0) {
            husky::base::log_msg("Finished BUC stage.\nStart post process...");
        }

        husky::ObjListFactory::drop_objlist("buc_list");

        husky::list_execute(post_list, {&post_ch}, {&agg_ch}, [&post_ch, &agg](Group& g) {
            auto& msg = post_ch.get(g);
            size_t pos = g.id().rfind("\t");
            std::string key = g.id().substr(0, pos);
            std::string w_idx = g.id().substr(pos + 1, g.id().length() - pos - 1);
            std::string hdfs_dest = ghdfs_dest + "/" + w_idx;
            std::string out = key + "\t" + std::to_string(msg.first) + "\t" + std::to_string(msg.second) + "\n";
            agg.update(1);
            husky::io::HDFS::Write(ghost, gport, out, hdfs_dest, husky::Context::get_global_tid());
        });
    }

    int total_num_write = agg.get_value();
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Total number of rows written to HDFS: " + std::to_string(total_num_write));
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("output");
    args.push_back("schema");
    args.push_back("select");
    args.push_back("group_sets");
    args.push_back("partition_factor");

    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cube_buc);
        return 0;
    }
    return 1;
}
