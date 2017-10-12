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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class CombineResults {

  final int NUM_POINTS_1 = 7;
  final int NUM_POINTS = 8;

  enum Clients{
    C100("100"),
    C1000("1000"),
    C4000("4000");

    private final String name;
    Clients(String name){
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private class ResultSet{
    Map<Integer, Double> clients100;
    Map<Integer, Double> clients1000;
    Map<Integer, Double> clients4000;

    int maxSeconds100;
    int maxSeconds1000;
    int maxSeconds4000;

    public ResultSet(){
      clients100 = new HashMap<>();
      clients1000 = new HashMap<>();
      clients4000 = new HashMap<>();
    }

    void add(int second, double avgTime, String path){
      if(path.startsWith("100_")){
        clients100.put(second, avgTime);
        maxSeconds100 = second;
      }else if(path.startsWith("1000_")){
        clients1000.put(second, avgTime);
        maxSeconds1000 = second;
      }else if(path.startsWith("4000_")){
        clients4000.put(second, avgTime);
        maxSeconds4000 = second;
      }
    }

    int getMaxSeconds(Clients clients){
      switch (clients){
        case C100:
          return maxSeconds100;
        case C1000:
          return maxSeconds1000;
        case C4000:
          return maxSeconds4000;
        default:
          return -1;
      }
    }

    String getResult(Clients clients, int second){
      Double res = null;
      switch (clients){
        case C100:
          res = clients100.get(second);
          break;
        case C1000:
          res = clients1000.get(second);
          break;
        case C4000:
          res = clients4000.get(second);
          break;
        default:
          break;
      }
      if(res == null){
        return "-";
      }
      return String.valueOf(res);
    }

  }

  private class HDFSInotifyResultsSet{
    ResultSet inotify_1k;
    ResultSet inotify_10k;
    ResultSet inotify_100k;
    public HDFSInotifyResultsSet(){
      inotify_1k = new ResultSet();
      inotify_10k = new ResultSet();
      inotify_100k = new ResultSet();
    }

    void add(final Path parent) throws IOException {
      Files.list(parent).forEach(new Consumer<Path>() {
        @Override
        public void accept(Path path) {
          try {
            List<String> lines = Files.readAllLines(path);
            for(String line : lines.subList(1, lines.size())){
              String[] splitString = line.split(",");
              int time = (int) Math.round(Double.valueOf(splitString[0])/1000.0);
              double avgTime = Double.valueOf(splitString[3]);
              if(parent.getFileName().startsWith("1k")){
                inotify_1k.add(time, avgTime, path.getFileName().toString());
              }else if(parent.getFileName().startsWith("10k")){
                inotify_10k.add(time, avgTime, path.getFileName().toString());
              }else if(parent.getFileName().startsWith("100k")){
                inotify_100k.add(time, avgTime, path.getFileName().toString());
              }
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }

    int getMaxSeconds(Clients clients){
      return Collections.max(Arrays.asList(inotify_1k.getMaxSeconds(clients),
          inotify_10k.getMaxSeconds(clients), inotify_100k.getMaxSeconds
              (clients)));
    }

    String getResult(Clients clients, int second){
      return inotify_1k.getResult(clients, second) + " " + inotify_10k
          .getResult(clients, second) + " " + inotify_100k.getResult(clients,
          second);
    }

  }


  private final String baseDir;
  public CombineResults(String baseDir){
    this.baseDir = baseDir;
  }

  public void run() throws IOException {
    ResultSet epipeResults = readePipeResults(Paths.get(baseDir, "epipe"));
    HDFSInotifyResultsSet hdfsInotifyResultsSet = readHDFSInotifyResults
        (Paths.get(baseDir, "hdfs_inotify"));

    writeCombinedResults(epipeResults, hdfsInotifyResultsSet, Clients.C100);
    writeCombinedResults(epipeResults, hdfsInotifyResultsSet, Clients.C1000);
    writeCombinedResults(epipeResults, hdfsInotifyResultsSet, Clients.C4000);

  }

  private void writeCombinedResults(ResultSet epipeResults ,
      HDFSInotifyResultsSet hdfsInotifyResultsSet, Clients clients)
      throws IOException {
    int max = epipeResults.getMaxSeconds(clients);
    int step = (int)((double)max/NUM_POINTS_1);

    Set<Integer> points = new HashSet<>();
    for(int i=1; i<=max; i+=step){
      points.add(i);
    }

    int lastp = max;

    int tmp = hdfsInotifyResultsSet.getMaxSeconds(clients);
    if(tmp > max){
      max = tmp;
    }

    step = (int)((double)max/NUM_POINTS);

    for(int i=lastp+step; i<=max; i+=step){
      points.add(i);
    }

    points.add(epipeResults.getMaxSeconds(clients));
    points.add(hdfsInotifyResultsSet.inotify_1k.getMaxSeconds(clients));
    points.add(hdfsInotifyResultsSet.inotify_10k.getMaxSeconds(clients));
    points.add(hdfsInotifyResultsSet.inotify_100k.getMaxSeconds(clients));

    List<Integer> pointsList = new ArrayList<>(points);
    Collections.sort(pointsList);

    BufferedWriter writer = Files.newBufferedWriter(Paths.get(baseDir,
        "lag_time_"+clients+"_clients.dat"));
    writer.write("#Time ePipe_avg_lag HDFS_Inotify_1k " +
        "HDFS_Inotify_10k HDFS_Inotify_100k\n");

    for(int p : pointsList){
      writer.write(p + " " + epipeResults.getResult(clients, p) + " " +
          hdfsInotifyResultsSet.getResult(clients, p) + "\n");
    }

    writer.close();
  }


  private ResultSet readePipeResults(Path epipeBaseDir) throws IOException {
    final ResultSet epipe = new ResultSet();
    Files.list(epipeBaseDir).forEach(new Consumer<Path>() {
      @Override
      public void accept(Path path) {
        try {
          List<String> lines = Files.readAllLines(path);
          for(String line : lines.subList(1, lines.size())){
            String[] splitString = line.split(",");
            int time = (int) Math.round(Double.valueOf(splitString[2])/1000.0);
            double avgTime = Double.valueOf(splitString[4]);
            epipe.add(time, avgTime, path.getFileName().toString());
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    return epipe;
  }

  private HDFSInotifyResultsSet readHDFSInotifyResults(Path hdfsInotifyBase) throws IOException {
    final HDFSInotifyResultsSet resultsSet = new HDFSInotifyResultsSet();
    Files.list(hdfsInotifyBase).forEach(new Consumer<Path>() {
      @Override
      public void accept(Path path) {
        try {
          resultsSet.add(path);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    return resultsSet;
  }

  public static void main(String[] args) throws IOException {
    if(args.length >= 1){
      new CombineResults(args[0]).run();
    }
  }
}
