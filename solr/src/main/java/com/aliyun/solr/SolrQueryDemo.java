package com.aliyun.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.CursorMarkParams;

import java.util.Arrays;

/**
 * 实例使用 {@link SolrAddDocumentDemo} 准备的数据进行各种查询demo演示
 *
 */
public class SolrQueryDemo {

    /**
     * 匹配所有文档记录
     */
    public static void matchAllQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("*:*");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 精确查找 "f1_s" 值为 "val99" 的记录
     */
    public static void termQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f1_s:val99");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 通配符号查询，"?" 表示一个任意字符， "*"表示 0个或多个任意字符
     */
    public static void wildcardQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f1_s:val?9 OR f1_s:val9*");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 模糊查询, 匹配 f1_s字段中，val99通过一个字符变化能匹配到的记录，比如 val9、val19、val29...val98、val99这种
     * f1_s:val99~1 表示可以允许有1个字符变化去匹配记录
     * f1_s:val99~ 表示默认可以有2个字符可变
     */
    public static void fuzzyQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f1_s:val99~1");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 短语查询，使用双引号引住，表示 "am Tom2"是一个完整的短语，要完整匹配
     */
    public static void phraseQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f11_t:\"am Tom2\"");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 近似查询，"I Tom2"~2 表示最多 I Tom2可以通过2次移动能匹配到的记录
     */
    public static void proximityQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f11_t:\"I Tom2\"~2");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 范围查询，中括号表示闭区间，大括号表示开区间
     * f6_i:[1 TO 3]  表示区间 [1,3]
     * f6_i:[90 TO *] 表示区间 [90,*]
     * f6_i:[* TO 3}  表示区间 [*,3)
     * f6_i:{1 TO 3}  表示区间 (1,3)
     */
    public static void rangeQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("f6_i:[* TO 3}");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 任意条件 AND/OR 组合查询
     */
    public static void queryMultiConditions(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("(f2_i:[1 TO 3] OR f3_l:[* TO 2]) AND (f1_s:val1 OR f1_s:val2) ");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * rows/start来进行分页, rows表示返回行数，start表示从第一个行开始取
     */
    public static void commonPagination(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            int page = 10;
            int start = 0;
            long total = 0;
            boolean firstRound = true;
            do {
                SolrQuery solrQuery = new SolrQuery("*:*");
                solrQuery.addSort("id", SolrQuery.ORDER.asc);
                solrQuery.setRows(page);
                solrQuery.setStart(start);
                QueryResponse response = client.query(collection, solrQuery);
                System.out.println(response);
                if(firstRound) {
                    total = response.getResults().getNumFound();
                    firstRound = false;
                }
                total -= page;
                start = start + page;
            }while(total >= 0);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * cursorMark分页查询，必须提供sort，并且必须sort必须包含id字段
     * 当cursorMark不再变化的时候，表示结果已经取完了
     *
     */
    public static void cursorMarkPagination(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            String nextCursorMark = CursorMarkParams.CURSOR_MARK_START;
            String currentMark = null;
            do {
                currentMark = nextCursorMark;
                SolrQuery solrQuery = new SolrQuery("*:*");
                solrQuery.addSort("id", SolrQuery.ORDER.asc);
                solrQuery.setRows(10);
                solrQuery.add(CursorMarkParams.CURSOR_MARK_PARAM, currentMark);
                QueryResponse response = client.query(collection, solrQuery);
                nextCursorMark = response.getNextCursorMark();
                System.out.println(response);
            } while (!nextCursorMark.equals(currentMark));
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 按照某个字段值进行统计匹配到的结果个数
     */
    public static void facetFieldDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.setFacet(true);
            solrQuery.addFacetField("f8_i");
            solrQuery.setFacetLimit(5); //表示统计返回的个数，特别是按照field单个统计时太多时
            solrQuery.setFacetMinCount(10); //表示统计结果大于10的统计值才返回
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response.getResults()); //查询返回的结果
            System.out.println(response.getFacetFields()); //根据查询结果，进行的facet统计
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 按照range query作为统计匹配结果个数返回
     * 这里是有facet query进行实现。还可以使用facetRange来简化
     */
    public static void facetRangeDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.setFacet(true);
            solrQuery.addFacetQuery("f8_i:[* TO 3]");
            solrQuery.addFacetQuery("f8_i:[4 TO 7]");
            solrQuery.addFacetQuery("f8_i:[8 TO *]");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response.getResults()); //查询返回的结果
            System.out.println(response.getFacetQuery()); // 按照range
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 使用function函数的例子
     * map(x,min,max,target,default) 表示x在min-max范围的时候，返回target值，否则返回default值
     * map(x,min,max,target) 表示x在min-max范围的时候，返回target值，否则原值返回
     * 更多函数，参考：https://lucene.apache.org/solr/guide/7_6/function-queries.html
     */
    public static void functionDemo(String collection, String zkhost, String zkroot) {
        try (CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.addField("id");
            solrQuery.addField("f7_i");
            solrQuery.addField("f8_i");
            solrQuery.addField("map(f7_i,0,15,-1,1)");
            solrQuery.addField("map(f6_i,0,15,-1)");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response.getResults());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据匹配的结果，统计min/max/sum/avg/count
     */
    public static void statsDemo(String collection, String zkhost, String zkroot) {
        try (CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            SolrQuery solrQuery = new SolrQuery("*:*");
            solrQuery.set("json.facet", "{max_f7:\"max(f7_i)\",min_f7:\"min(f7_i)\",sum_f7:\"sum(f7_i)\",avg_f7:\"avg(f7_i)\"}");
            QueryResponse response = client.query(collection, solrQuery);
            System.out.println(response.getResults()); //查询返回的结果
            System.out.println(response.getResponse().get("facets"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 按照query删除匹配的文档
     * 操作这个的时候要小心，如果query写错了删了数据就非常麻烦了，不建议使用这个
     * 除非你测试开发非常明确需要用这个
     */
    public static void deleteByQueryDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            UpdateResponse response = client.deleteByQuery(collection,"f9_i:[0 TO 5]");
            System.out.println(response);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 按照id删除某个document
     */
    public static void deleteByIdDemo(String collection, String zkhost, String zkroot) {
        try(CloudSolrClient client = new CloudSolrClient.Builder().withZkChroot(zkroot).withZkHost(zkhost).build()) {
            UpdateResponse response1 = client.deleteById("1");
            System.out.println(response1);
            UpdateResponse response2 = client.deleteById(Arrays.asList("2","3"));
            System.out.println(response2);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //Use CloudSolrClient to add document
        String zkroot = "/";
        String zkhost = "localhost:9983";
        String collection = "solrdemo";
        matchAllQueryDemo(collection, zkhost, zkroot);
        termQueryDemo(collection, zkhost, zkroot);
        wildcardQueryDemo(collection, zkhost, zkroot);
        fuzzyQueryDemo(collection, zkhost, zkroot);
        phraseQueryDemo(collection, zkhost, zkroot);
        proximityQueryDemo(collection, zkhost, zkroot);
        rangeQueryDemo(collection, zkhost, zkroot);
        queryMultiConditions(collection, zkhost, zkroot);
        commonPagination(collection, zkhost, zkroot);
        cursorMarkPagination(collection, zkhost, zkroot);
        facetFieldDemo(collection, zkhost, zkroot);
        facetRangeDemo(collection, zkhost, zkroot);
        functionDemo(collection, zkhost, zkroot);
        statsDemo(collection, zkhost, zkroot);
        deleteByQueryDemo(collection, zkhost, zkroot);
        deleteByIdDemo(collection, zkhost, zkroot);
    }

}
