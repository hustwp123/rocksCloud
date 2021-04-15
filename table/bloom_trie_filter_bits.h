#pragma once

#include <array>
#include "tries/path_decomposed_trie.hpp"
#include "tries/vbyte_string_pool.hpp"
#include <time.h>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/filter_policy.h"
#include "full_filter_bits_builder.h"

namespace rocksdb {

class Slice;
using rocksdb::succinct::util::char_range;
using rocksdb::succinct::util::stl_string_adaptor;

class BloomTrieFilterBitsBuilder : public FilterBitsBuilder {
  // Create a filter that union with hash filter and opt filter
  // +----------------------------------------------------------------+
  // |             hash filter data                                   |
  // +----------------------------------------------------------------+
  // |             opt filter  data                                   |
  // +----------------------------------------------------------------+
 public:
  explicit BloomTrieFilterBitsBuilder(const size_t bits_per_key,
                                 const size_t num_probes, bool with_opt) 
        : full_builder(bits_per_key, num_probes), with_opt_(with_opt) {
      if(bits_per_key == 0) {
        with_full_ = false;
      }

      assert(with_full_ || with_opt);
  }

  ~BloomTrieFilterBitsBuilder() { }

  virtual void AddKey(const Slice& key) override {
    if(with_full_) {
      full_builder.AddKey(key);
    }
    if(with_opt_) {
      opt_builder.AddKey(key);
    }

    num_added_ ++;
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override;


  // Calculate space for new filter. This is reverse of CalculateNumEntry.
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines, uint32_t* bloom_size, uint32_t* opt_size) {
      uint32_t space = 0;
      if(with_full_) {
        *bloom_size = full_builder.CalculateSpace(num_entry, total_bits, num_lines);
        space += (*bloom_size + CACHE_LINE_SIZE - 1) & (~(CACHE_LINE_SIZE - 1));
      }
      if(with_opt_) {
        *opt_size = opt_builder.CalculateSpace(num_entry);
        space += *opt_size;
      }
      space += 8; // 最后8个字节存放长度
      return space;
  }

 private:
  OtLexPdtBloomBitsBuilder opt_builder;
  FullFilterBitsBuilder full_builder;

  size_t num_added_ = 0;
  bool with_full_ = true;
  bool with_opt_ = true;

  // No Copy allowed
  BloomTrieFilterBitsBuilder(const BloomTrieFilterBitsBuilder&);
  void operator=(const BloomTrieFilterBitsBuilder&);
};

}  // namespace rocksdb
