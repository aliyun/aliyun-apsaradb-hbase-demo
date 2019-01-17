package com.aliyun.solr;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SolrAddDocumentDemo {

    public static class DocumentGenerator{

        public SolrInputDocument nextDocument(long currentNum){
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", currentNum);
            doc.addField("f1_s","val"+currentNum);
            doc.addField("f2_i",currentNum/100);
            doc.addField("f3_l",currentNum);
            doc.addField("f4_d",currentNum);
            doc.addField("f5_f",currentNum);
            doc.addField("f6_i",currentNum%100);
            doc.addField("f7_i",currentNum%30);
            doc.addField("f8_i",currentNum%8);
            doc.addField("f9_i",currentNum%300);
            doc.addField("f10_s","val"+currentNum);
            doc.addField("f11_t", "Hello, I am Tom" + currentNum + ", I am "
                    + currentNum % 100 + " years old.");
            return doc;
        }

    }

    public static class IndexTester implements Runnable{
        private CloudSolrClient cloudSolrClient ;
        private String collection;
        private long start;
        private long stop;
        private long currentNum;
        private DocumentGenerator docGen;
        private int batchSize = 100;
        private List<SolrInputDocument> buffer;
        private int commitWithinMs;
        private long completed = 0;


        public IndexTester(CloudSolrClient cloudSolrClient,String collection,long start ,
                           long stop,DocumentGenerator docGen, int batchSize, int commitWithinMs){
            this.cloudSolrClient = cloudSolrClient;
            this.collection = collection;
            this.start = start;
            this.stop = stop;
            this.docGen = docGen;
            currentNum = this.start;
            this.batchSize = batchSize;
            buffer = new ArrayList<SolrInputDocument>();
            this.commitWithinMs = commitWithinMs;
        }

        @Override
        public void run() {
            System.out.println("InsertTester: start " + this.start + " stop " + this.stop + " startted");
            while(currentNum < stop){
                if(buffer.size() >= batchSize){
                    flushWithRetry();
                }
                SolrInputDocument doc = docGen.nextDocument(currentNum);
                buffer.add(doc);
                currentNum++;
            }
            if(buffer.size() > 0 ){
                flushWithRetry();
            }
            System.out.println("InsertTester: start " + this.start + " stop " + this.stop + " finish");
        }

        private int sleepInterval = 1;
        private void flushWithRetry() {
            try {
                if (buffer.size() == 0) {
                    return;
                }
                UpdateResponse response = this.cloudSolrClient.add(collection, buffer, commitWithinMs);
                if (response.getStatus() != 0) {
                    throw new Exception("response with status " + response.getStatus() + ", detail: " + response);
                }
                completed += buffer.size();
                System.out.println("completed " + completed + "/" + (stop - start) + " of [" + start + "," + stop + ")");
                buffer.clear();
                sleepInterval = 1;
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                //print & sleep for a while
                try {
                    sleepInterval = sleepInterval * 2 > 120 ? 120 : sleepInterval;
                    System.out.println("sleep " + sleepInterval + " s  before retry.");
                    TimeUnit.SECONDS.sleep(sleepInterval);
                } catch (InterruptedException e1) {
                }
            }
        }

    }

    public static void main(String[] args) {
        //Use CloudSolrClient to add document
        String zkroot = "/";
        String zkhost = "localhost:9983";

        //threadNum, totalDocNum, collectionName, batchSize, commitWithinMs
        args = new String[]{"1", "100", "solrdemo", "100", "1000"};
        if (args == null || args.length != 5) {
            System.out.println("error input args: <threadNum> <totalDocNum> <collection> <batchSize> <commitWithinMs>");
        }

        long threadNum = Long.parseLong(args[0]);
        long totalDocNum = Long.parseLong(args[1]);
        long avgCount = totalDocNum / threadNum;
        String collection = args[2];
        int batchSize = Integer.parseInt(args[3]);
        int commitWithinMs = Integer.parseInt(args[4]);

        long last = 0;
        List<IndexTester> indexTesters = new ArrayList<IndexTester>();
        List<CloudSolrClient> clients = new ArrayList<CloudSolrClient>();
        while (totalDocNum > 0) {
            if (totalDocNum - avgCount >= 0) {
                long start = last;
                long stop = start + avgCount;
                totalDocNum = totalDocNum - avgCount;
                CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot)
                        .withZkHost(zkhost).build();
                IndexTester tester = new IndexTester(client, collection, start, stop,
                        new DocumentGenerator(), batchSize, commitWithinMs);
                clients.add(client);
                indexTesters.add(tester);
                last = stop;
            } else {
                long start = last;
                long stop = start + totalDocNum;
                totalDocNum = 0;
                CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot)
                        .withZkHost(zkhost).build();
                IndexTester tester = new IndexTester(client, collection, start, stop,
                        new DocumentGenerator(), batchSize, commitWithinMs);
                clients.add(client);
                indexTesters.add(tester);
                last = stop;
            }
        }

        waitForCompleted(indexTesters);
        System.out.println("finish all insert SUCCESSFUL ~");
        for (CloudSolrClient c : clients) {
            try {
                c.close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static void waitForCompleted(List<IndexTester> indexTesters){
        ExecutorService threadPool = ExecutorUtil.newMDCAwareCachedThreadPool(
                new SolrjNamedThreadFactory("SolrAddDocumentDemo-ThreadPool"));
        List<Future> futures = new ArrayList<>();
        for(IndexTester indexTester : indexTesters) {
            Future f = threadPool.submit(indexTester);
            futures.add(f);
        }
        threadPool.shutdown();
        for(Future f : futures){
            try {
                f.get();
                System.out.println("one thread completed. ");
            } catch (Exception e) {
                System.err.println("one thread wait failed.");
                e.printStackTrace();
            }
        }

    }

}
