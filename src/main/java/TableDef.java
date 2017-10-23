/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public interface TableDef {
  // INode Table Columns
  String TABLE_NAME = "hdfs_inodes";
  String ID = "id";
  String NAME = "name";
  String PARENT_ID = "parent_id";
  String PARTITION_ID = "partition_id";
  String IS_DIR = "is_dir";
  String MODIFICATION_TIME = "modification_time";
  String ACCESS_TIME = "access_time";
  String USER_ID = "user_id";
  String GROUP_ID = "group_id";
  String PERMISSION = "permission";
  String CLIENT_NAME = "client_name";
  String CLIENT_MACHINE = "client_machine";
  String CLIENT_NODE = "client_node";
  String GENERATION_STAMP = "generation_stamp";
  String HEADER = "header";
  String SYMLINK = "symlink";
  String QUOTA_ENABLED = "quota_enabled";
  String UNDER_CONSTRUCTION = "under_construction";
  String SUBTREE_LOCKED = "subtree_locked";
  String SUBTREE_LOCK_OWNER = "subtree_lock_owner";
  String META_ENABLED = "meta_enabled";
  String SIZE = "size";
  String FILE_STORED_IN_DB = "file_stored_in_db";
  String LOGICAL_TIME = "logical_time";
}
