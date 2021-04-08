// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <array>
#include "tries/path_decomposed_trie.hpp"
#include "tries/vbyte_string_pool.hpp"
#include <time.h>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/filter_policy.h"
// #define USE_PDT_BUILDER
namespace rocksdb {

class Slice;
using rocksdb::succinct::util::char_range;
using rocksdb::succinct::util::stl_string_adaptor;

struct rocksdb_slice_adaptor {
  char_range operator()(Slice const &s) const {
    const uint8_t *buf = reinterpret_cast<const uint8_t *>(s.data());
    const uint8_t *end = buf + s.size(); // dadd the null terminator
    return char_range(buf, end);
  }
};

// xp
class OtLexPdtBloomBitsBuilder : public FilterBitsBuilder {
 public:
  explicit OtLexPdtBloomBitsBuilder()
      :ot_pdt() {
//    fprintf(stderr, "constructing OtLexPdtBloomBitsBuilder()\n");
  }

  // No Copy allowed
  OtLexPdtBloomBitsBuilder(const OtLexPdtBloomBitsBuilder&) = delete;
  void operator=(const OtLexPdtBloomBitsBuilder&) = delete;

  ~OtLexPdtBloomBitsBuilder() override {}

  virtual void AddKey(const Slice& key) override {
//    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::AddKey() idpaeq\n");
    std::string key_string(key.data(), key.data()+key.size());
#ifdef USE_PDT_BUILDER
    builder.add_key(visitor, key_string, stl_string_adaptor());
#else
    key_strings_.push_back(key_string);
#endif
  }

  uint32_t CalculateSpace(const int num_entry) {
    return static_cast<uint32_t>(CalculateByteSpace());
  }

//   virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
// #ifndef USE_PDT_BUILDER
// //    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::Finish() 8qpeye\n");
//     // generate a compacted trie and get essential data
//     assert(key_strings_.size() > 0);
//     key_strings_.erase(unique(key_strings_.begin(),
//                               key_strings_.end()),
//                        key_strings_.end()); //xp, for now simply dedup keys

// //    fprintf(stdout, "DEBUG w7zvbg key_strings_.size: %lu\n", key_strings_.size());

//     //auto chrono_start = std::chrono::system_clock::now();
//     ot_pdt.construct_compacted_trie(key_strings_, false); // ot_pdt.pub_ are inited
//     key_strings_.clear();
// #else
//     builder.finish(visitor);
//     ot_pdt.finish_essentia(visitor);
// #endif
//     // auto chrono_end = std::chrono::system_clock::now();
//     // std::chrono::microseconds elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(chrono_end-chrono_start);
//     // std::cout << "DEBUG 3yb6fo Finis() ot_pdt use " << key_strings_.size() << " keys build raw trie takes(us) " <<
//     //           elapsed_us.count() << std::endl;

//     // get the byte size of key_strings_
//     uint64_t ot_lex_pdt_byte_size = CalculateByteSpace();
//     assert(ot_lex_pdt_byte_size > 0);
//     uint64_t buf_byte_size = ot_lex_pdt_byte_size + 5;
// //    fprintf(stderr, "DEBUG 10dk3 compacted trie byte size: %lu\n", buf_byte_size);

//     char* contents = new char[buf_byte_size];
//     memset(contents, 0, buf_byte_size);
//     assert(contents);

//     //xp, be compatible with new bloom filter (full filter)
//     // See BloomFilterPolicy::GetBloomBitsReader re: metadata
//     // -1 = Marker for newer Bloom implementations
//     char new_impl = static_cast<char>(-1);
// //    contents[byte_size] = static_cast<char>(-1);
//     // 0 = Marker for this sub-implementation, ot lex pdt
//     char sub_impl = static_cast<char>(80); // 'P'
// //    contents[byte_size+1] = static_cast<char>(5);
//     // just for padding
//     // when full filter: num_probes (and 0 in upper bits for 64-byte block size)
//     char fake_num_probes = static_cast<char>(7);
// //    contents[byte_size+2] = static_cast<char>(7);
//     // rest of metadata (2 chars) are paddings

//     // format essential data with format
//     // vec<uint16_t>,
//     // vec<uint16_t>,
//     // vec<uint8_t>,
//     // vec<uint8_t>,
//     // vec<uint64_t>,
//     // uint64_t
//     // char
//     // char
//     // char
//     // & ot lex pdt byte size
//     // target buffer
// //    fprintf(stdout, "DEBUG a3gcn6 in otFinish sizes for string,label,branch,char,bit,size:%ld,%ld,%ld,%ld,%ld,%ld\n",
// //            ot_pdt.pub_m_centroid_path_string.size(),
// //            ot_pdt.pub_m_labels.size(),
// //            ot_pdt.pub_m_centroid_path_branches.size(),
// //            ot_pdt.pub_m_branching_chars.size(),
// //            ot_pdt.pub_m_bp_m_bits.size(),
// //            ot_pdt.pub_m_bp_m_size);
//     PutIntoCharArray(ot_pdt.pub_m_centroid_path_string,
//                      ot_pdt.pub_m_labels,
//                      ot_pdt.pub_m_centroid_path_branches,
//                      ot_pdt.pub_m_branching_chars,
//                      ot_pdt.pub_m_bp_m_bits,
//                      ot_pdt.pub_m_bp_m_size,
//                      new_impl,
//                      sub_impl,
//                      fake_num_probes,
//                      buf_byte_size,
//                      contents);

// //    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_string.size(); i++) {
// //      fprintf(stdout, "Pstring:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_string[i]);
// //    }
// //    for (size_t i = 0; i < ot_pdt.pub_m_labels.size(); i++) {
// //      fprintf(stdout, "Plabel:%ld,%d\n", i, ot_pdt.pub_m_labels[i]);
// //    }
// //    for (size_t i = 0; i < ot_pdt.pub_m_centroid_path_branches.size(); i++) {
// //      fprintf(stdout, "Pbranch:%ld,%d\n", i, ot_pdt.pub_m_centroid_path_branches[i]);
// //    }
// //    for (size_t i = 0; i < ot_pdt.pub_m_branching_chars.size(); i++) {
// //      fprintf(stdout, "Pchar:%ld,%d\n", i, ot_pdt.pub_m_branching_chars[i]);
// //    }
// //    for (size_t i = 0; i < ot_pdt.pub_m_bp_m_bits.size(); i++) {
// //      fprintf(stdout, "Pbit:%ld,%ld\n", i, ot_pdt.pub_m_bp_m_bits[i]);
// //    }

//     assert(sizeof(ot_pdt.pub_m_bp_m_size) != 0);

//     // return a Slice with data and its byte length
//     const char* const_data = contents;
//     buf->reset(const_data);
//     std::cout << "Essential_Filter size: " << buf_byte_size << std::endl; 

//     return Slice(contents, buf_byte_size);
// //=============
// //    uint32_t len_with_metadata =
// //        CalculateSpace(static_cast<uint32_t>(hash_entries_.size()));
// //    char* data = new char[len_with_metadata];
// //    memset(data, 0, len_with_metadata);
// //
// //    assert(data);
// //    assert(len_with_metadata >= 5);
// //
// //    uint32_t len = len_with_metadata - 5;
// //    if (len > 0) {
// //      AddAllEntries(data, len);
// //    }

//     // See BloomFilterPolicy::GetBloomBitsReader re: metadata
//     // -1 = Marker for newer Bloom implementations
// //    data[len] = static_cast<char>(-1);
//     // 0 = Marker for this sub-implementation
// //    data[len + 1] = static_cast<char>(0);
//     // num_probes (and 0 in upper bits for 64-byte block size)
// //    data[len + 2] = static_cast<char>(num_probes_);
//     // rest of metadata stays zero

// //    const char* const_data = data;
// //    buf->reset(const_data);
// //    hash_entries_.clear();
// //    return Slice(data, len_with_metadata);
//   }

virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
#ifndef USE_PDT_BUILDER
//    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::Finish() 8qpeye\n");
    // generate a compacted trie and get essential data
    assert(key_strings_.size() > 0);
    key_strings_.erase(unique(key_strings_.begin(),
                              key_strings_.end()),
                       key_strings_.end()); //xp, for now simply dedup keys

//    fprintf(stdout, "DEBUG w7zvbg key_strings_.size: %lu\n", key_strings_.size());

    //auto chrono_start = std::chrono::system_clock::now();
#ifdef USE_FULL_OT_PDT
    ot_pdt.bulk_load(key_strings_, rocksdb::succinct::tries::stl_string_adaptor());;
#else
    ot_pdt.construct_compacted_trie(key_strings_, false); // ot_pdt.pub_ are inited
#endif
    key_strings_.clear();
#else
    builder.finish(visitor);
#ifdef USE_FULL_OT_PDT
    ot_pdt.instance(visitor);
#else
    ot_pdt.finish_essentia(visitor);
#endif
#endif

#ifdef USE_FULL_OT_PDT
    using rocksdb::succinct::EncodeArgs;
    char* contents = nullptr;
    EncodeArgs arg(contents);
    arg.only_size = true;
    ot_pdt.Encode(&arg);

    uint64_t buf_byte_size = arg.size;
    contents = new char[buf_byte_size];
    arg.dst = contents;
    arg.only_size = false;
    ot_pdt.Encode(&arg);
#else
    uint64_t ot_lex_pdt_byte_size = CalculateByteSpace();
    assert(ot_lex_pdt_byte_size > 0);
    uint64_t buf_byte_size = ot_lex_pdt_byte_size + 5;
//    fprintf(stderr, "DEBUG 10dk3 compacted trie byte size: %lu\n", buf_byte_size);

    char* contents = new char[buf_byte_size];
    memset(contents, 0, buf_byte_size);
    assert(contents);
    char new_impl = static_cast<char>(-1);
    char sub_impl = static_cast<char>(80); // 'P'
    char fake_num_probes = static_cast<char>(7);

    PutIntoCharArray(ot_pdt.pub_m_centroid_path_string,
                     ot_pdt.pub_m_labels,
                     ot_pdt.pub_m_centroid_path_branches,
                     ot_pdt.pub_m_branching_chars,
                     ot_pdt.pub_m_bp_m_bits,
                     ot_pdt.pub_m_bp_m_size,
                     new_impl,
                     sub_impl,
                     fake_num_probes,
                     buf_byte_size,
                     contents);

    assert(sizeof(ot_pdt.pub_m_bp_m_size) != 0);
#endif

    // return a Slice with data and its byte length
    const char* const_data = contents;
    buf->reset(const_data);
    fprintf(stdout, "Filter size: %ld, contents ptr: %p\n", buf_byte_size, contents);
    return Slice(contents, buf_byte_size);
  }
//   virtual Slice FinishWithString(std::string& buf)  {
// //    fprintf(stderr, "in OtLexPdtBloomBitsBuilder::Finish() 8qpeye\n");
//     // generate a compacted trie and get essential data
// #ifndef USE_STRING_FILTER
//     std::cout << "No define USE_STRING_FILTER" << std::endl;
//     abort();
// #endif

// #ifdef USE_PDT_BUILDER
//     builder.finish(visitor);
//     ot_pdt.instance(visitor);
// #else
//     assert(key_strings_.size() > 0);
//     key_strings_.erase(unique(key_strings_.begin(),
//                               key_strings_.end()),
//                        key_strings_.end()); //xp, for now simply dedup keys
//     ot_pdt.bulk_load(key_strings_, rocksdb::succinct::tries::stl_string_adaptor());
//     key_strings_.clear();
// #endif
//     // rocksdb::succinct::tries::path_decomposed_trie<rocksdb::succinct::tries::vbyte_string_pool, true>
//     //  new_ot_pdt;
//     // const char *buf_data = buf.c_str();
//     // new_ot_pdt.Decode(&buf_data);

//     // for(size_t i = 0; i < key_strings_.size(); i ++) {
//     //  if(new_ot_pdt[i] != ot_pdt[i]) { std::cout << "Decode or Encode error" << std::endl; }
//     //  if(new_ot_pdt.index(key_strings_[i]) != i ) { std::cout << "OT pdt error" << std::endl; }
//     // }

//     // key_strings_.clear();
//     using rocksdb::succinct::EncodeArgs;
//     char *char_buf;
//     EncodeArgs arg(char_buf);
//     arg.only_size = true;
//     ot_pdt.Encode(&arg);
//     std::cout << "Filter size: " << arg.size << std::endl;
    
//     return Slice(buf);
//   }

  // ot lex pdt used byte size
  uint64_t CalculateByteSpace() {
    // calculate the byte size of format buf
    return (ot_pdt.pub_m_centroid_path_string.size()+ot_pdt.pub_m_labels.size())*2 +
        ot_pdt.pub_m_centroid_path_branches.size()+ ot_pdt.pub_m_branching_chars.size() +
        ot_pdt.pub_m_bp_m_bits.size() * 8 + 8 +
        5 * 4; // sizes of above vectors
  }

  // put essential data into char[], and this char[] will be stored in the Slice
  // of a particular Table.
  //  NOTE: total used byte == byte_size (ot lex pdt byte size) + 5
  char* PutIntoCharArray(std::vector<uint16_t>& v1, std::vector<uint16_t>& v2,
                         std::vector<uint8_t>& v3, std::vector<uint8_t>& v4,
                         std::vector<uint64_t>& v5, uint64_t num,
                         char new_impl, char sub_impl, char fake_num_probes,
                         uint64_t& byte_size, char*& buf) {
//    byte_size =
//        (v1.size() + v2.size()) * 2 + v3.size() + v4.size() + v5.size() * 8 + 8;
//    byte_size += 5 * 4 + 5;  //指明4个vector的size + 5 padding chars
//
    buf = new char[byte_size];
    memset(buf, 0, byte_size);

    uint32_t* p = (uint32_t*)buf;
    *p = v1.size();
    uint16_t* p1 = (uint16_t*)(buf + 4);
    for (uint32_t i = 0; i < v1.size(); i++) {
      *p1 = v1[i];
      p1++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2);
    *p = v2.size();
    p1 = (uint16_t*)(buf + 4 + v1.size() * 2 + 4);
    for (uint32_t i = 0; i < v2.size(); i++) {
      *p1 = v2[i];
      p1++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2);
    *p = v3.size();
    uint8_t* p2 = (uint8_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4);
    for (uint32_t i = 0; i < v3.size(); i++) {
      *p2 = v3[i];
      p2++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size());
    *p = v4.size();
    p2 = (uint8_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size() + 4);
    for (uint32_t i = 0; i < v4.size(); i++) {
      *p2 = v4[i];
      p2++;
    }

    p = (uint32_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                    v3.size() + 4 + v4.size());
    *p = v5.size();
    // fprintf(stderr, "DEBUG h8qd8z PutIntoCharArray m_bits.size(): %u\n", *p);
    uint64_t* p4 = (uint64_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                               v3.size() + 4 + v4.size() + 4);
    for (uint32_t i = 0; i < v5.size(); i++) {
      *p4 = v5[i];
      p4++;
    }

    uint64_t* p3 = (uint64_t*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                               v3.size() + 4 + v4.size() + 4 + v5.size() * 8);
    *p3 = num;
//    fprintf(stderr, "DEBUG yz92dt PutIntoCharArray num: %lu, *p3:%lu\n", num, *p3);

    // new bloom filter implementation indicators for GetBloomBitsReader
    char* pc1 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8);
    *pc1 = new_impl;
    char* pc2 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1);
    *pc2 = sub_impl;
    char* pc3 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1);
    *pc3 = fake_num_probes;
//    char* pc4 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
//                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1 + 1);
//    *pc4 = static_cast<char>(0);
//    char* pc5 = (char*)(buf + 4 + v1.size() * 2 + 4 + v2.size() * 2 + 4 +
//                        v3.size() + 4 + v4.size() + 4 + v5.size() * 8 + 8 + 1 + 1 + 1 + 1);
//    *pc5 = static_cast<char>(0);

    return buf;
  }


  std::vector<std::string> key_strings_;  // vector for Slice.data
#ifdef USE_PDT_BUILDER
  std::string last_string;
  typedef rocksdb::succinct::tries::path_decomposed_trie<succinct::tries::vbyte_string_pool, true> Trie_t;
  typedef Trie_t::centroid_builder_visitor builder_visitor_t;
  builder_visitor_t visitor;
  size_t builder_keys = 0;
  rocksdb::succinct::tries::compacted_trie_builder<builder_visitor_t> builder;
#endif

  // a compacted trie, NO need for a ot lex pdt yet
  rocksdb::succinct::tries::path_decomposed_trie<rocksdb::succinct::tries::vbyte_string_pool, true> ot_pdt;
};



class FullFilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullFilterBitsBuilder(const size_t bits_per_key,
                                 const size_t num_probes);

  ~FullFilterBitsBuilder();

  virtual void AddKey(const Slice& key) override;

  // Create a filter that for hashes [0, n-1], the filter is allocated here
  // When creating filter, it is ensured that
  // total_bits = num_lines * CACHE_LINE_SIZE * 8
  // dst len is >= 5, 1 for num_probes, 4 for num_lines
  // Then total_bits = (len - 5) * 8, and cache_line_size could be calculated
  // +----------------------------------------------------------------+
  // |              filter data with length total_bits/8              |
  // +----------------------------------------------------------------+
  // |                                                                |
  // | ...                                                            |
  // |                                                                |
  // +----------------------------------------------------------------+
  // | ...                | num_probes : 1 byte | num_lines : 4 bytes |
  // +----------------------------------------------------------------+
  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override;

  // Calculate num of entries fit into a space.
  virtual int CalculateNumEntry(const uint32_t space) override;

  // Calculate space for new filter. This is reverse of CalculateNumEntry.
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

 private:
  friend class FullFilterBlockTest_DuplicateEntries_Test;
  size_t bits_per_key_;
  size_t num_probes_;
  std::vector<uint32_t> hash_entries_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);

  // No Copy allowed
  FullFilterBitsBuilder(const FullFilterBitsBuilder&);
  void operator=(const FullFilterBitsBuilder&);
};

}  // namespace rocksdb
