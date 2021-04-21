//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/block_cache_tier.h"

#include <regex>
#include <utility>
#include <vector>

#include "logging/logging.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"
#include "utilities/persistent_cache/block_cache_tier_file.h"

namespace rocksdb {

//
// BlockCacheImpl
//
Status BlockCacheTier::Open() {
  Status status;

  WriteLock _(&lock_);

  assert(!size_);

  // Check the validity of the options
  status = opt_.ValidateSettings();
  assert(status.ok());
  if (!status.ok()) {
    Error(opt_.log, "Invalid block cache options");
    return status;
  }

  // Create base directory or cleanup existing directory
  status = opt_.env->CreateDirIfMissing(opt_.path);
  if (!status.ok()) {
    Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  // Create base/<cache dir> directory
  status = opt_.env->CreateDir(GetCachePath());
  if (!status.ok()) {
    // directory already exists, clean it up
    status = CleanupCacheFolder(GetCachePath());
    assert(status.ok());
    if (!status.ok()) {
      Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
            status.ToString().c_str());
      return status;
    }
  }

  // Create base/<table_cache dir> directory
  status = opt_.env->CreateDir(GetTableCachePath());
  if (!status.ok()) {
    // directory already exists, clean it up
    status = CleanupCacheFolder(GetTableCachePath());
    assert(status.ok());
    if (!status.ok()) {
      Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
            status.ToString().c_str());
      return status;
    }
  }

  // create a new file
  assert(!cache_file_);
  status = NewCacheFile();
  if (!status.ok()) {
    Error(opt_.log, "Error creating new file %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  assert(cache_file_);

  // create a new table_cache file
  assert(!table_cache_file_);
  status = NewTableCacheFile();
  if (!status.ok()) {
    Error(opt_.log, "Error creating new file %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  assert(table_cache_file_);

  if (opt_.pipeline_writes) {
    assert(!insert_th_.joinable());
    insert_th_ = port::Thread(&BlockCacheTier::InsertMain, this);
  }

  return Status::OK();
}

bool IsCacheFile(const std::string& file) {
  // check if the file has .rc suffix
  // Unfortunately regex support across compilers is not even, so we use simple
  // string parsing
  size_t pos = file.find(".");
  if (pos == std::string::npos) {
    return false;
  }

  std::string suffix = file.substr(pos);
  return suffix == ".rc";
}

Status BlockCacheTier::CleanupCacheFolder(const std::string& folder) {
  std::vector<std::string> files;
  Status status = opt_.env->GetChildren(folder, &files);
  if (!status.ok()) {
    Error(opt_.log, "Error getting files for %s. %s", folder.c_str(),
          status.ToString().c_str());
    return status;
  }

  // cleanup files with the patter :digi:.rc
  for (auto file : files) {
    if (IsCacheFile(file)) {
      // cache file
      Info(opt_.log, "Removing file %s.", file.c_str());
      status = opt_.env->DeleteFile(folder + "/" + file);
      if (!status.ok()) {
        Error(opt_.log, "Error deleting file %s. %s", file.c_str(),
              status.ToString().c_str());
        return status;
      }
    } else {
      ROCKS_LOG_DEBUG(opt_.log, "Skipping file %s", file.c_str());
    }
  }
  return Status::OK();
}

Status BlockCacheTier::Close() {
  // stop the insert thread
  if (opt_.pipeline_writes && insert_th_.joinable()) {
    InsertOp op(/*quit=*/true);
    insert_ops_.Push(std::move(op));
    insert_th_.join();
  }

  // stop the writer before
  writer_.Stop();
  table_writer_.Stop();
  // clear all metadata
  WriteLock _(&lock_);
  metadata_.Clear();
  table_metadata_.Clear();
  return Status::OK();
}

template <class T>
void Add(std::map<std::string, double>* stats, const std::string& key,
         const T& t) {
  stats->insert({key, static_cast<double>(t)});
}

PersistentCache::StatsType BlockCacheTier::Stats() {
  std::map<std::string, double> stats;
  Add(&stats, "persistentcache.blockcachetier.bytes_piplined",
      stats_.bytes_pipelined_.Average());
  Add(&stats, "persistentcache.blockcachetier.bytes_written",
      stats_.bytes_written_.Average());
  Add(&stats, "persistentcache.blockcachetier.bytes_read",
      stats_.bytes_read_.Average());
  Add(&stats, "persistentcache.blockcachetier.insert_dropped",
      stats_.insert_dropped_);
  Add(&stats, "persistentcache.blockcachetier.cache_hits", stats_.cache_hits_);
  Add(&stats, "persistentcache.blockcachetier.cache_misses",
      stats_.cache_misses_);
  Add(&stats, "persistentcache.blockcachetier.cache_errors",
      stats_.cache_errors_);
  Add(&stats, "persistentcache.blockcachetier.cache_hits_pct",
      stats_.CacheHitPct());
  Add(&stats, "persistentcache.blockcachetier.cache_misses_pct",
      stats_.CacheMissPct());
  Add(&stats, "persistentcache.blockcachetier.read_hit_latency",
      stats_.read_hit_latency_.Average());
  Add(&stats, "persistentcache.blockcachetier.read_miss_latency",
      stats_.read_miss_latency_.Average());
  Add(&stats, "persistentcache.blockcachetier.write_latency",
      stats_.write_latency_.Average());

  auto out = PersistentCacheTier::Stats();
  out.push_back(stats);
  return out;
}

Status BlockCacheTier::Insert(const Slice& key, const char* data,
                              const size_t size, bool is_meta_block,
                              std::string) {
  // update stats
  stats_.bytes_pipelined_.Add(size);

  if (opt_.pipeline_writes) {
    // off load the write to the write thread
    insert_ops_.Push(InsertOp(
        key.ToString(), std::move(std::string(data, size)), is_meta_block));
    return Status::OK();
  }

  assert(!opt_.pipeline_writes);
  return InsertImpl(key, Slice(data, size), is_meta_block);
}

void BlockCacheTier::InsertMain() {
  while (true) {
    InsertOp op(insert_ops_.Pop());

    if (op.signal_) {
      // that is a secret signal to exit
      break;
    }

    size_t retry = 0;
    Status s;
    while ((s = InsertImpl(Slice(op.key_), Slice(op.data_), op.is_meta_block_))
               .IsTryAgain()) {
      if (retry > kMaxRetry) {
        break;
      }

      // this can happen when the buffers are full, we wait till some buffers
      // are free. Why don't we wait inside the code. This is because we want
      // to support both pipelined and non-pipelined mode
      buffer_allocator_.WaitUntilUsable();
      retry++;
    }

    if (!s.ok()) {
      stats_.insert_dropped_++;
    }
  }
}

Status BlockCacheTier::InsertImpl(const Slice& key, const Slice& data,
                                  bool is_meta_block) {
  // pre-condition
  assert(key.size());
  assert(data.size());
  assert(cache_file_);

  StopWatchNano timer(opt_.env, /*auto_start=*/true);

  WriteLock _(&lock_);

  if (!is_meta_block) {
    LBA lba;
    if (metadata_.Lookup(key, &lba)) {
      // the key already exists, this is duplicate insert
      return Status::OK();
    }

    while (!cache_file_->Append(key, data, &lba)) {
      if (!cache_file_->Eof()) {
        ROCKS_LOG_DEBUG(opt_.log, "Error inserting to cache file %d",
                        cache_file_->cacheid());
        stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
        return Status::TryAgain();
      }

      assert(cache_file_->Eof());
      Status status = NewCacheFile();
      if (!status.ok()) {
        return status;
      }
    }

    // Insert into lookup index
    BlockInfo* info = metadata_.Insert(key, lba);
    assert(info);
    if (!info) {
      return Status::IOError("Unexpected error inserting to index");
    }

    // insert to cache file reverse mapping
    cache_file_->Add(info);

    // update stats
    stats_.bytes_written_.Add(data.size());
    stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::OK();
  } else {
    // printf("key.size==%ld    data.size==%ld\n",key.size(),data.size());
    LBA lba;
    if (table_metadata_.Lookup(key, &lba)) {
      // the key already exists, this is duplicate insert
      return Status::OK();
    }
    while (!table_cache_file_->Append(key, data, &lba)) {
      if (!table_cache_file_->Eof()) {
        ROCKS_LOG_DEBUG(opt_.log, "Error inserting to cache file %d",
                        table_cache_file_->cacheid());
        stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
        return Status::TryAgain();
      }

      assert(table_cache_file_->Eof());
      Status status = NewTableCacheFile();
      if (!status.ok()) {
        return status;
      }
    }

    // Insert into lookup index
    BlockInfo* info = table_metadata_.Insert(key, lba);
    assert(info);
    if (!info) {
      return Status::IOError("Unexpected error inserting to index");
    }

    // insert to cache file reverse mapping
    table_cache_file_->Add(info);

    // update stats
    stats_.bytes_written_.Add(data.size());
    stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::OK();
  }
}

Status BlockCacheTier::Lookup(const Slice& key, std::unique_ptr<char[]>* val,
                              size_t* size, std::string) {
  StopWatchNano timer(opt_.env, /*auto_start=*/true);

  bool find_in_meta_block = false;

  LBA lba;
  bool status;
  status = table_metadata_.Lookup(key, &lba);
  if (!status) {
    bool status2;
    status2 = metadata_.Lookup(key, &lba);
    if (!status2) {
      stats_.cache_misses_++;
      stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
      // printf("lookup failed1\n");
      return Status::NotFound("blockcache: key not found");
    } else {
      find_in_meta_block = false;
    }
  } else {
    // printf("get in meta_block\n");
    find_in_meta_block = true;
  }
  if (!find_in_meta_block) {
    BlockCacheFile* const file = metadata_.Lookup(lba.cache_id_);
    if (!file) {
      // this can happen because the block index and cache file index are
      // different, and the cache file might be removed between the two lookups
      stats_.cache_misses_++;
      stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
      return Status::NotFound("blockcache: cache file not found");
    }

    assert(file->refs_);

    std::unique_ptr<char[]> scratch(new char[lba.size_]);
    Slice blk_key;
    Slice blk_val;

    status = file->Read(lba, &blk_key, &blk_val, scratch.get());
    --file->refs_;
    if (!status) {
      stats_.cache_misses_++;
      stats_.cache_errors_++;
      stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
      return Status::NotFound("blockcache: error reading data");
    }

    assert(blk_key == key);

    val->reset(new char[blk_val.size()]);
    memcpy(val->get(), blk_val.data(), blk_val.size());
    *size = blk_val.size();

    stats_.bytes_read_.Add(*size);
    stats_.cache_hits_++;
    stats_.read_hit_latency_.Add(timer.ElapsedNanos() / 1000);
    // printf("lookup get in data block\n");
    return Status::OK();
  } else {
    BlockCacheFile* const file = table_metadata_.Lookup(lba.cache_id_);
    if (!file) {
      // this can happen because the block index and cache file index are
      // different, and the cache file might be removed between the two lookups
      stats_.cache_misses_++;
      stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
      return Status::NotFound("blockcache: cache file not found");
    }

    assert(file->refs_);

    std::unique_ptr<char[]> scratch(new char[lba.size_]);
    Slice blk_key;
    Slice blk_val;

    status = file->Read(lba, &blk_key, &blk_val, scratch.get());
    --file->refs_;
    if (!status) {
      stats_.cache_misses_++;
      stats_.cache_errors_++;
      stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
      return Status::NotFound("blockcache: error reading data");
    }

    assert(blk_key == key);

    val->reset(new char[blk_val.size()]);
    memcpy(val->get(), blk_val.data(), blk_val.size());
    *size = blk_val.size();

    stats_.bytes_read_.Add(*size);
    stats_.cache_hits_++;
    stats_.read_hit_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::OK();
  }
}

bool BlockCacheTier::Erase(const Slice& key) {
  WriteLock _(&lock_);
  LBA lba;
  bool status;
  status = metadata_.Lookup(key, &lba);
  if (status) {
    BlockInfo* info = metadata_.Remove(key);
    assert(info);
    delete info;
  }
  status = table_metadata_.Lookup(key, &lba);
  if (status) {
    BlockInfo* info = table_metadata_.Remove(key);
    assert(info);
    delete info;
  }

  return true;
}

Status BlockCacheTier::NewTableCacheFile() {
  lock_.AssertHeld();

  TEST_SYNC_POINT_CALLBACK("BlockCacheTier::NewTableCacheFile:DeleteDir",
                           (void*)(GetTableCachePath().c_str()));

  std::unique_ptr<WriteableCacheFile> f(new WriteableCacheFile(
      opt_.env, &table_buffer_allocator_, &table_writer_, GetTableCachePath(),
      table_writer_cache_id_, opt_.cache_file_size, opt_.log));

  bool status = f->Create(opt_.enable_direct_writes, opt_.enable_direct_reads);
  if (!status) {
    return Status::IOError("Error creating file");
  }

  Info(opt_.log, "Created cache file %d", table_writer_cache_id_);

  table_writer_cache_id_++;
  table_cache_file_ = f.release();

  // insert to cache files tree
  status = table_metadata_.Insert(table_cache_file_);
  assert(status);
  if (!status) {
    Error(opt_.log, "Error inserting to metadata");
    return Status::IOError("Error inserting to metadata");
  }

  return Status::OK();
}

Status BlockCacheTier::NewCacheFile() {
  lock_.AssertHeld();

  TEST_SYNC_POINT_CALLBACK("BlockCacheTier::NewCacheFile:DeleteDir",
                           (void*)(GetCachePath().c_str()));

  std::unique_ptr<WriteableCacheFile> f(new WriteableCacheFile(
      opt_.env, &buffer_allocator_, &writer_, GetCachePath(), writer_cache_id_,
      opt_.cache_file_size, opt_.log));

  bool status = f->Create(opt_.enable_direct_writes, opt_.enable_direct_reads);
  if (!status) {
    return Status::IOError("Error creating file");
  }

  Info(opt_.log, "Created cache file %d", writer_cache_id_);

  writer_cache_id_++;
  cache_file_ = f.release();

  // insert to cache files tree
  status = metadata_.Insert(cache_file_);
  assert(status);
  if (!status) {
    Error(opt_.log, "Error inserting to metadata");
    return Status::IOError("Error inserting to metadata");
  }

  return Status::OK();
}

bool BlockCacheTier::Reserve(const size_t size, bool is_meta_block) {
  WriteLock _(&lock_);

  if (!is_meta_block) {
    assert(size_ <= opt_.cache_size);

    if (size + size_ <= opt_.cache_size) {
      // there is enough space to write
      size_ += size;
      return true;
    }

    assert(size + size_ >= opt_.cache_size);
    // there is not enough space to fit the requested data
    // we can clear some space by evicting cold data

    const double retain_fac = (100 - kEvictPct) / static_cast<double>(100);
    while (size + size_ > opt_.cache_size * retain_fac) {
      std::unique_ptr<BlockCacheFile> f(metadata_.Evict());
      if (!f) {
        // nothing is evictable
        return false;
      }
      assert(!f->refs_);
      uint64_t file_size;
      if (!f->Delete(&file_size).ok()) {
        // unable to delete file
        return false;
      }

      assert(file_size <= size_);
      size_ -= file_size;
    }

    size_ += size;
    assert(size_ <= opt_.cache_size * 0.9);
    return true;
  } else {
    table_size_ += size;
    return true;
  }
}

Status NewPersistentCache(Env* const env, const std::string& path,
                          const uint64_t size,
                          const std::shared_ptr<Logger>& log,
                          const bool optimized_for_nvm,
                          std::shared_ptr<PersistentCache>* cache) {
  if (!cache) {
    return Status::IOError("invalid argument cache");
  }

  auto opt = PersistentCacheConfig(env, path, size, log);
  if (optimized_for_nvm) {
    // the default settings are optimized for SSD
    // NVM devices are better accessed with 4K direct IO and written with
    // parallelism
    opt.enable_direct_writes = true;
    opt.writer_qdepth = 4;
    opt.writer_dispatch_size = 4 * 1024;
  }

  auto pcache = std::make_shared<BlockCacheTier>(opt);
  Status s = pcache->Open();

  if (!s.ok()) {
    return s;
  }

  *cache = pcache;
  return s;
}

// zyh
//

Status NewPersistentmyCache(Env* const env, const std::string& path,
                            const uint64_t size,
                            const std::shared_ptr<Logger>& log,
                            const bool optimized_for_nvm,
                            std::shared_ptr<PersistentCache>* cache) {
  if (!cache) {
    return Status::IOError("invalid argument cache");
  }

  auto opt = PersistentCacheConfig(env, path, size, log);
  if (optimized_for_nvm) {
    // the default settings are optimized for SSD
    // NVM devices are better accessed with 4K direct IO and written with
    // parallelism
    opt.enable_direct_writes = true;
    opt.writer_qdepth = 4;
    opt.writer_dispatch_size = 4 * 1024;
  }

  auto pcache = std::make_shared<myCache>(opt);
  Status s = pcache->Open();

  if (!s.ok()) {
    return s;
  }

  *cache = pcache;
  return s;
}

// wp

Status SST_space::Get(const std::string key, std::unique_ptr<char[]>* data,
                      size_t* size) {
  MutexLock _(&lock);
  if (!cache.count(key)) {
    return Status::NotFound("Mycache not found");
  }
  DLinkedNode* node = cache[key];
  moveToHead(node);
  data->reset(new char[node->value.size]);
  if(node->value.size>SPACE_SIZE)
  {
    fprintf(stderr,"get size=%ld\n",node->value.size);
  }
  
  if(node->value.offset.size()>1)
  {
    fprintf(stderr,"get num>1\n");
  }
  *size = node->value.size;
  size_t cur = 0;
  for (uint32_t i = 0; i < (node->value.offset.size() - 1); i++) {
    ssize_t t =
        pread(fd, data->get() + cur, SPACE_SIZE, begin + node->value.offset[i]);
    if (t != SPACE_SIZE) {
      return Status::IOError();
    }
    cur += SPACE_SIZE;
  }
  ssize_t left_size = node->value.size % SPACE_SIZE == 0
                      ? SPACE_SIZE
                      : node->value.size % SPACE_SIZE;
  int index = node->value.offset.size() - 1;
  ssize_t t = pread(fd, data->get() + cur, left_size,
                    begin + node->value.offset[index]);
  if (t != left_size) {
    return Status::IOError();
  }
  return Status::OK();
}

void SST_space::Put(const std::string& key, const std::string& value,
                    uint64_t& out, bool is_meta) {
  MutexLock _(&lock);
  if (value.size() == 0) {
    return;
  }
  uint32_t need_num = value.size() / SPACE_SIZE;
  need_num += value.size() % SPACE_SIZE == 0 ? 0 : 1;
  if (need_num > all_num)// || need_num > 1) 
  {
    return;
  }
  // if(need_num>1)
  // {
  //   fprintf(stderr,"Put need_num>1\n ");
  // }
  DLinkedNode* node;
  if (!cache.count(key))  // key不存在 创建新节点
  {
    node = new DLinkedNode();
    node->key = key;
    node->value.offset.clear();
    node->out = is_meta ? 1 : 0;
    cache[key] = node;
    addToHead(node);
    while (need_num > empty_num) {
      DLinkedNode* tail_ = getTail();
      if (tail_->out) {
        tail_->out--;
        moveToHead(tail_);
        continue;
      }
      //fprintf(stderr,"out\n");
      out++;
      DLinkedNode* removed = removeTail();
      cache.erase(removed->key);
      removeRecord(&(removed->value));
      delete removed;
    }
  } else {
    node = cache[key];
    node->out = is_meta ? 1 : 0;
    moveToHead(node);
    removeRecord(&(node->value));
    while (need_num > empty_num) {
      DLinkedNode* tail_ = getTail();
      if (tail_->out) {
        tail_->out--;
        moveToHead(tail_);
        continue;
      }
      //fprintf(stderr,"out\n");
      out++;
      DLinkedNode* removed = removeTail();
      cache.erase(removed->key);
      removeRecord(&(removed->value));
      delete removed;
    }
  }
  node->value.size = value.size();
  empty_num -= need_num;

  if (empty_nodes.size() >= need_num) {
    //fprintf(stderr,"get from empty_nodes need_num=%d empty_nodes.size()=%ld\n",need_num,empty_nodes.size());
    for (uint32_t i = 0; i < empty_nodes.size() && i < need_num; i++) {
      node->value.offset.push_back(empty_nodes[i] * SPACE_SIZE);
      bit_map[empty_nodes[i]]=1;
    }
    empty_nodes.clear();
  } else {
    
    for (uint32_t i = 0; i < empty_nodes.size() && i < need_num; i++) {
      node->value.offset.push_back(empty_nodes[i] * SPACE_SIZE);
      bit_map[empty_nodes[i]]=1;
    }
    need_num-=empty_nodes.size();
    empty_nodes.clear();
    
    uint32_t num = 0, j = (last + 1) % bit_map.size();
    while (j != last) {
      //fprintf(stderr,"get from bit map need num=%d\n",need_num);
      if (!bit_map[j]) {
        bit_map[j] = 1;
        node->value.offset.push_back(j * SPACE_SIZE);
        num++;
        if (num >= need_num) {
          last=j;
          break;
        }
      }
      j=(j+1)%bit_map.size();
    }
  }

  // node->value.offset.push_back(0);
  // //取块
  // uint32_t num = 0, j = 0;
  // while (j < bit_map.size()) {
  //   if (!bit_map[j]) {
  //     bit_map[j] = 1;
  //     node->value.offset.push_back(j * SPACE_SIZE);
  //     num++;
  //     if (num == need_num) {
  //       break;
  //     }
  //   }
  //   j++;
  // }

  //写块
  size_t cur = 0;
  for (uint32_t i = 0; i < node->value.offset.size() - 1; i++) {
    ssize_t t = pwrite(fd, value.c_str() + cur, SPACE_SIZE,
                       begin + node->value.offset[i]);
    if (t != SPACE_SIZE) {
      fprintf(stderr,"pwrite error\n");
      return;
    }
    cur += SPACE_SIZE;
  }
  
  ssize_t left_size =
      value.size() % SPACE_SIZE == 0 ? SPACE_SIZE : value.size() % SPACE_SIZE;
  int index = node->value.offset.size() - 1;
  ssize_t t = pwrite(fd, value.c_str() + cur, left_size,
                     begin + node->value.offset[index]);
  if (t != left_size) {
    fprintf(stderr,"pwrite error\n");
    return;
  }
}

Status myCache::Insert(const Slice& key, const char* data, const size_t size,
                       bool is_meta, std::string fname) {
  // Insert2(std::string(key.data(), key.size()), std::string(data, size),
  // fname); return Status::OK(); if(is_meta)
  // {
  //   fprintf(stderr,"insert size=%ld\n",size);
  // }
  if (opt_.pipeline_writes) {
    insert_ops_.Push(myInsertOp(key.ToString(),
                                std::move(std::string(data, size)), is_meta,
                                std::move(fname)));
    return Status::OK();
  }
  InsertImpl(std::string(key.data(), key.size()), std::string(data, size),
             is_meta, fname);
  return Status::OK();
}
void myCache::InsertMain() {
  while (true) {
    myInsertOp op(insert_ops_.Pop());

    if (op.signal_) {
      // that is a secret signal to exit
      break;
    }
    InsertImpl(op.key_, op.value_, op.is_meta, op.fname_);
  }
}

Status myCache::Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                       size_t* size, std::string fname) {
  // MutexLock _(&lock_);
  std::string skey(key.data(), key.size());
  int index = getIndex(fname);
  Status s = v[index].Get(skey, data, size);
  return s;
}

Status myCache::InsertImpl(const std::string& key, const std::string& value,
                           bool is_meta, std::string& fname) {
  // MutexLock _(&lock_);
  int index = getIndex(fname, true);
  v[index].Put(key, value, outall, is_meta);
  return Status::OK();
}

int myCache::getIndex(std::string fname, bool) {
  if (fname.size() == 0) {
    return 0;
  }
  int i = fname.size() - 1;
  while (fname[i] != '.') {
    i--;
  }
  i--;
  int j = i;
  int sum = 0;
  while (fname[i] >= '0' && fname[i] <= '9' && i >= 0) {
    i--;
  }
  i++;
  while (fname[i] == '0') {
    i++;
  }
  while (j >= i) {
    sum = sum * 10 + fname[i] - '0';
    i++;
  }

  return sum % NUM;
}

Status myCache::Open() {
  // MutexLock _(&lock_);
  std::string path = opt_.path;
  path += "/pcache_file";
  // std::string path2 = path + "Get_sst";
  // std::string path3 = path + "Put_sst";

  // fp = fopen(path2.c_str(), "w+");
  // fp2 = fopen(path3.c_str(), "w+");
  fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
  lseek(fd, opt_.cache_size, SEEK_SET);
  int t = -1;
  ssize_t tt = write(fd, &t, sizeof(int));
  if (tt < 0) {
    return Status::IOError();
  }

  NUM = opt_.cache_size / SST_SIZE;
  // v.resize(NUM);
  for (uint64_t i = 0; i < NUM; i++) {
    v[i].Set_Par(fd, SST_SIZE / SPACE_SIZE, i * SST_SIZE);
  }
  if (opt_.pipeline_writes) {
    insert_th_ = port::Thread(&myCache::InsertMain, this);
  }
  return Status::OK();
}
Status myCache::Close() {
  // MutexLock _(&lock_);
  if (opt_.pipeline_writes && insert_th_.joinable()) {
    myInsertOp op(/*quit=*/true);
    insert_ops_.Push(std::move(op));
    insert_th_.join();
  }
  close(fd);
  // fclose(fp);
  // fclose(fp2);
  uint64_t all_empty_num = 0;
  for (uint64_t i = 0; i < NUM; i++) {
    all_empty_num += v[i].empty_num;
  }

  fprintf(stderr, "/n/n\n all_empty_num=%ld \n", all_empty_num);
  fprintf(stderr, "/n/n\n outall=%ld\n", outall);
  return Status::OK();
}
bool myCache::Erase(const Slice&) { return true; }
bool myCache::Reserve(const size_t, bool) { return true; }

bool myCache::IsCompressed() { return opt_.is_compressed; }

std::string myCache::GetPrintableOptions() const { return opt_.ToString(); }

}  // namespace rocksdb

#endif  // ifndef ROCKSDB_LITE
