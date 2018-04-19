import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.client.InfiniteTrumpetEventStreamer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;
import java.util.Map;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TrumpetBench extends AbstractAppLauncher{
  
  @Override
  protected int internalRun() throws Exception {
    SummaryStatistics rollingAverage = new SummaryStatistics();
    SummaryStatistics averageTime = new SummaryStatistics();
    
    long startTime = System.currentTimeMillis();
    long t = System.currentTimeMillis();
    
    System.out.println("#totalTime,TxId,rollingAvg,avgAcc");
    
    InfiniteTrumpetEventStreamer trumpetEventStreamer = new InfiniteTrumpetEventStreamer(getCuratorFrameworkKafka(), getTopic());
    Iterator<Map<String, Object>> it = trumpetEventStreamer.iterator();
    while (it.hasNext()){
      long currentTime = System.currentTimeMillis();
      Map<String, Object> event = it.next();
      if(event.get(EventAndTxId.FIELD_EVENTTYPE).equals("CLOSE")){
        long closeTime = (Long) event.get("timestamp");
        long closeLag = currentTime - closeTime;
        averageTime.addValue(closeLag);
        rollingAverage.addValue(closeLag);
        if (currentTime - t >= 1000) {
          System.out.println((System.currentTimeMillis()-startTime) + "," +
              event.get(EventAndTxId.FIELD_TXID)+ "," + rollingAverage.getMean()
              + "," + averageTime.getMean());
          rollingAverage.clear();
          t = currentTime;
        }
      }
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new TrumpetBench(), args);
    System.exit(res);
  }
}
