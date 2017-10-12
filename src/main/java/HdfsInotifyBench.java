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

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import java.io.IOException;
import java.net.URI;

public class HdfsInotifyBench {

  public static void main(String[] args)
      throws IOException, InterruptedException,
      MissingEventsException {

    Configuration configuration = new Configuration();

    configuration.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
    configuration.set("dfs.nameservices", "mycluster");
    configuration.set("dfs.namenode.rpc-address.mycluster.nn1",
        "hadoop4:11001");
    configuration.set("dfs.namenode.rpc-address.mycluster.nn2",
        "hadoop6:11001");
    configuration.set("dfs.client.failover.proxy.provider.mycluster", "org" +
        ".apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    configuration.set("fs.hdfs.impl",
        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

    URI uri = URI.create("hdfs://hadoop4:11001");
    HdfsAdmin admin = new HdfsAdmin(uri,
        configuration);


    SummaryStatistics rollingAverage = new SummaryStatistics();
    SummaryStatistics averageTime = new SummaryStatistics();

    DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();

    long startTime = System.currentTimeMillis();
    long t = System.currentTimeMillis();

    System.out.println("#totalTime,TxBehind,rollingAvg,avgAcc");

    while (true) {
      EventBatch batch = eventStream.take();

      long txIdsBehind = eventStream.getTxidsBehindEstimate();
      for (Event event : batch.getEvents()) {
        long currentTime = System.currentTimeMillis();
        switch (event.getEventType()) {
          case CLOSE:
            Event.CloseEvent closeEvent = (Event.CloseEvent) event;
            long closeLag = currentTime - closeEvent.getTimestamp();
            averageTime.addValue(closeLag);
            rollingAverage.addValue(closeLag);
            if (currentTime - t >= 1000) {
              System.out.println((System.currentTimeMillis()-startTime) + "," +
                  txIdsBehind+ "," + rollingAverage.getMean()
                  + "," + averageTime.getMean());
              rollingAverage.clear();
              t = currentTime;
            }
          case CREATE:
          case UNLINK:
          case APPEND:
          case RENAME:
          default:
            break;
        }
      }
    }

  }
}
