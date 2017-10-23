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
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AutoIncrementTest {

  static {
    // raise log level for clusterj to WARN
    Logger.getLogger("com.mysql.clusterj").setLevel(Level.WARNING);
  }


  String DROP_TABLE = "drop table if exists `test_table`;";
  String CREATE_TABLE = "create table `test_table` (`pk` int auto_increment, " +
      "int_col int, str_col varchar(250), PRIMARY KEY (`pk`));";

  @PersistenceCapable(table = "test_table")
  public interface TableDTO {
    @PrimaryKey
    @Column(name = "pk")
    int getPK();
    void setPK(int primaryKey);


    @Column(name = "int_col")
    int getIntCol();
    void setIntCol(int val);


    @Column(name = "str_col")
    String getStrCol();
    void setStrCol(String val);
  }

  @Option(name = "-batchSize", usage = "Auto Increment Batch Size. Default is" +
      " 1")
  private int batchSize = 1;
  @Option(name = "-numThreads", usage = "Number of threads. Default is 100.")
  private int numThreads = 100;
  @Option(name = "-incThreads", usage = "Incremental Threads. Default is " +
      "false.")
  private boolean incThreads = false;
  @Option(name = "-incThreadsStart", usage = "Incremental Threads start point" +
      ". Default is 10.")
  private int incThreadsStart = 10;
  @Option(name = "-incThreadsIncrement", usage = "Incremental Threads " +
      "increment. Default is 10.")
  private int incThreadsIncrement = 10;
  @Option(name = "-schema", usage = "DB schemma name. Default is test")
  static private String schema = "hop_mahmoud";
  @Option(name = "-dbHost", usage = "com.mysql.clusterj.connectstring. Default is bbc2")
  static private String dbHost = "bbc2.sics.se";
  @Option(name = "-totalOps", usage = "Total operations to perform. Default is 1000. Recommended 1 million or more")
  private static long totalOps = 1000;
  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;
  @Option(name = "-init", usage = "Drop and create the table.")
  private boolean init = false;

  SessionFactory sf = null;


  private static AtomicInteger opsCompleted = new AtomicInteger(0);
  private static AtomicInteger successfulOps = new AtomicInteger(0);
  private static AtomicInteger failedOps = new AtomicInteger(0);


  private void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      System.exit(-1);
    }

    if (help) {
      parser.printUsage(System.err);
      System.exit(0);
    }
  }

  public void setUpDBConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", dbHost);
    props.setProperty("com.mysql.clusterj.database", schema);
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    props.setProperty("com.mysql.clusterj.connect.autoincrement.batchsize",
        String.valueOf(batchSize));
    sf = ClusterJHelper.getSessionFactory(props);
  }


  public double runWorkers(int numThreads) throws InterruptedException, IOException {
    DBAutoIncrementWriter[] workers = new DBAutoIncrementWriter[numThreads];
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      DBAutoIncrementWriter worker = new DBAutoIncrementWriter();
      workers[i]=worker;
    }

    long startTime = System.currentTimeMillis();
    for(DBAutoIncrementWriter worker : workers){
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      Thread.sleep(1000);
    }
    double elapsed = (System.currentTimeMillis() - startTime) / 1000.0;
    return successfulOps.get()/elapsed;
  }

  public class DBAutoIncrementWriter implements Runnable {

    public DBAutoIncrementWriter() {
    }

    @Override
    public void run() {
      Session dbSession = sf.getSession();
      for (int i = 0; i < totalOps; i++) {
        try {
          TableDTO row = dbSession.newInstance(TableDTO.class);
          row.setIntCol(i);
          row.setStrCol("stringcolumn" + i);
          dbSession.makePersistent(row);

          successfulOps.incrementAndGet();
          dbSession.release(row);
        } catch (Throwable e) {
          failedOps.incrementAndGet();
          e.printStackTrace();
        }
        opsCompleted.incrementAndGet();
      }
      dbSession.close();
    }
  }

  private void initTable() throws SQLException {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUser("hop");
    dataSource.setPassword("hop");
    dataSource.setServerName(dbHost);
    dataSource.setDatabaseName(schema);
    Connection conn = dataSource.getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute(DROP_TABLE);
    stmt.execute(CREATE_TABLE);
    stmt.close();
    conn.close();
  }

  public void run(String[] args) throws Exception {
    parseArgs(args);
    setUpDBConnection();
    if(init) {
      initTable();
    }
    double currThroughput = runWorkers(numThreads);
    System.out.println(numThreads + ", " + currThroughput);
  }

  public static void main(String[] args) throws Exception {
    new AutoIncrementTest().run(args);
  }
}
