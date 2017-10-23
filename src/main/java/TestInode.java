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

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.util.Properties;

public class TestInode implements TableDef{

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PARTITION_ID)
  public interface InodeDTO {
    @PrimaryKey
    @Column(name = PARTITION_ID)
    int getPartitionId();
    void setPartitionId(int partitionId);

    //id of the parent inode
    @PrimaryKey
    @Column(name = PARENT_ID)
    @Index(name = "pidex")
    int getParentId();     // id of the inode
    void setParentId(int parentid);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    @Column(name = ID)
    @Index(name = "inode_idx")
    int getId();     // id of the inode
    void setId(int id);

    @Column(name = IS_DIR)
    byte getIsDir();
    void setIsDir(byte isDir);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();
    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();
    void setATime(long modificationTime);

    // Inode
    @Column(name = USER_ID)
    int getUserID();
    void setUserID(int userID);

    // Inode
    @Column(name = GROUP_ID)
    int getGroupID();
    void setGroupID(int groupID);

    // Inode
    @Column(name = PERMISSION)
    short getPermission();
    void setPermission(short permission);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();
    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();
    void setClientMachine(String clientMachine);

    @Column(name = CLIENT_NODE)
    String getClientNode();
    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = GENERATION_STAMP)
    int getGenerationStamp();
    void setGenerationStamp(int generation_stamp);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();
    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();
    void setSymlink(String symlink);

    @Column(name = QUOTA_ENABLED)
    byte getQuotaEnabled();
    void setQuotaEnabled(byte quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    byte getUnderConstruction();
    void setUnderConstruction(byte underConstruction);

    @Column(name = SUBTREE_LOCKED)
    byte getSubtreeLocked();
    void setSubtreeLocked(byte locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();
    void setSubtreeLockOwner(long leaderId);

    @Column(name = META_ENABLED)
    byte getMetaEnabled();
    void setMetaEnabled(byte metaEnabled);

    @Column(name = SIZE)
    long getSize();
    void setSize(long size);

    @Column(name = FILE_STORED_IN_DB)
    byte getFileStoredInDd();
    void setFileStoredInDd(byte isFileStoredInDB);

    @Column(name = LOGICAL_TIME)
    int getLogicalTime();     // id of the inode
    void setLogicalTime(int logicalTime);
  }



  public void run(){
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", "bbc2.sics.se");
    props.setProperty("com.mysql.clusterj.database", "hop_mahmoud");
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    SessionFactory sf = ClusterJHelper.getSessionFactory(props);

    Session session = sf.getSession();
    InodeDTO dto = session.newInstance(InodeDTO.class);
    dto.setPartitionId(2);
    dto.setParentId(2);
    dto.setName("f2");

    dto.setLogicalTime(100);
    session.savePersistent(dto);
    session.release(dto);
    //session.close();
  }
}
