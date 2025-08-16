// Copyright (c) 2024 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cassert>
#include <iostream>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

int main() {
  // 定义数据库指针和选项
  leveldb::DB* db;
  leveldb::Options options;

  // 如果数据库不存在，则创建它
  options.create_if_missing = true;

  // 定义数据库路径
  const std::string dbpath = "./testdb";

  // --- 1. 打开数据库 ---
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  if (!status.ok()) {
    std::cerr << "Unable to open database " << dbpath << ": " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "Database opened successfully." << std::endl;

  // --- 2. 基本的 Put 和 Get 操作 ---
  std::string key1 = "key1";
  std::string value1 = "value1";

  // 写入键值对
  status = db->Put(leveldb::WriteOptions(), key1, value1);
  assert(status.ok());
  std::cout << "Put: " << key1 << " -> " << value1 << std::endl;

  // 读取键值对
  std::string read_value;
  status = db->Get(leveldb::ReadOptions(), key1, &read_value);
  assert(status.ok());
  assert(read_value == value1);
  std::cout << "Get: " << key1 << " -> " << read_value << " (Verified)" << std::endl;

  // --- 3. 使用 WriteBatch 进行原子操作 ---
  leveldb::WriteBatch batch;
  std::string key2 = "key2";
  std::string value2 = "value2";

  // 在一个批处理中删除 key1 并添加 key2
  batch.Delete(key1);
  batch.Put(key2, value2);

  // 应用批处理
  status = db->Write(leveldb::WriteOptions(), &batch);
  assert(status.ok());
  std::cout << "WriteBatch applied: Deleted " << key1 << ", Put " << key2 << " -> " << value2 << std::endl;

  // 验证 key1 是否已被删除
  status = db->Get(leveldb::ReadOptions(), key1, &read_value);
  assert(status.IsNotFound());
  std::cout << "Verified: " << key1 << " is not found." << std::endl;

  // 验证 key2 是否已添加
  status = db->Get(leveldb::ReadOptions(), key2, &read_value);
  assert(status.ok());
  assert(read_value == value2);
  std::cout << "Verified: " << key2 << " -> " << read_value << std::endl;

  // --- 4. 使用迭代器遍历数据库 ---
  std::cout << "\nIterating through all key-value pairs:" << std::endl;
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << " : " << it->value().ToString() << std::endl;
  }
  assert(it->status().ok()); // 检查迭代过程中是否发生错误
  delete it;

  // --- 5. 关闭数据库 ---
  delete db;
  std::cout << "\nDatabase closed." << std::endl;

  // (可选) 销毁数据库，清理文件
  // leveldb::DestroyDB(dbpath, leveldb::Options());
  // std::cout << "Database destroyed." << std::endl;

  return 0;
}