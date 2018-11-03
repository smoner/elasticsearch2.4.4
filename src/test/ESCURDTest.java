import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESCURDTest extends SpringTestCase {

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    DriverManagerDataSource driverManagerDataSource;


    public static final String[] ES_FIELD_NAMES = new String[]{
            "id","vtenant_id","vpurchase_enterprise_id","vpurchase_code","vsupply_enterprise_id",
            "vsupply_erp_id","vsupply_name","vmaterial_id","vmaterial_name","vmaterial_code",
            "nprice","vsrc_mark","vunit_name","vsrc_system","vadd_type",
            "vsrc_billcode","vmaterial_type","vpurchase_erp_name","vpurchase_org_id","nsum",
            "ntax","jsupply","jcondition","jprice","jmaterial",
            "dbilldate","ts","vupdate_mark","vsupply_prod_id","vsupply_prod_name",
            "vsupply_prod_code","vmaterial_spec","vmaterial","vcondition","vprice",
            "vsrc","vsupply_code","vsupply","vmaterial_erp_id","vpurchase_name",
            "vpurchase","vsupply_tenant_id","vpurchase_erp_id","vcurrency","vpurchase_erp_code",
            "vpurchase_enterprise_code","vpurchase_enterprise_name","create_time","nprice_notax","nmny",
            "nmny_notax","jpurchase"
    };


    //分页信息
    public static final String PAGE_PAGEINDEX = "pageIndex" ;
    public static final String PAGE_PAGESIZE = "pageSize" ;
    public static final String PAGE_TOTALCOUNT = "totalCount" ;
    public static final String PAGE_TOTALPAGE = "totalPage" ;
    public static final String PAGE_FROMINDEX = "fromIndex" ;
    public static final String RESULT = "result" ;

    public static final Map<String,String> ES_JSON_KEYMAP = new HashMap<String,String>();
    static {
        ES_JSON_KEYMAP.put("jsupply","jsupply");
        ES_JSON_KEYMAP.put("jcondition","jcondition");
        ES_JSON_KEYMAP.put("jprice","jprice");
        ES_JSON_KEYMAP.put("jmaterial","jmaterial");
        ES_JSON_KEYMAP.put("jpurchase","jpurchase");
    }

    public static final Map<String,String> ES_DOUBLE_KEYMAP = new HashMap<String,String>();
    static {
        ES_DOUBLE_KEYMAP.put("nprice_notax","nprice_notax");
        ES_DOUBLE_KEYMAP.put("nmny","nmny");
        ES_DOUBLE_KEYMAP.put("nmny_notax","nmny_notax");
        ES_DOUBLE_KEYMAP.put("nsum","nsum");
        ES_DOUBLE_KEYMAP.put("nprice","nprice");
        ES_DOUBLE_KEYMAP.put("ntax","ntax");
    }

    @Test
    public void testInsertFromMap(){
        try{
            Connection connection = driverManagerDataSource.getConnection();
            String sql = " select * from cpu_biprice where id = 1623998 ";
            ResultSet rs = connection.createStatement().executeQuery(sql);
            Map<String,Object> objectMap = new HashMap<String,Object>();
            while (rs.next()){
                for(String key :ES_FIELD_NAMES){
                    String object = rs.getString(key);
                    if(StringUtils.isNotBlank(object)){
                        if(ES_JSON_KEYMAP.containsKey(key)){
                            JSONObject object1 = JSONObject.parseObject(rs.getString(key));
                            objectMap.put(key,object1);
                        }else if(ES_DOUBLE_KEYMAP.containsKey(key)){
                            objectMap.put(key,rs.getDouble(key));
                        }else{
                            objectMap.put(key,rs.getString(key));
                        }
                    }
                }
            }
            this.insertFromMap("biprice4","price",objectMap);
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }

    public String insertFromMap(String index, String type, Map<String, Object> data) {
        try{
            Client client = elasticsearchTemplate.getClient();
            IndexResponse actionGet = client
                    .prepareIndex(index, type)
                    .setSource(data)
                    //.setId("1") //自己设置了id，也可以使用ES自带的，但是看文档说，ES的会因为删除id发生变动。
                    .setId(String.valueOf(data.get("id")))
                    .execute().actionGet();
            actionGet.isCreated();
            return  "新增成功!";
        }catch (Exception e){
            return  "新增失败!详细日志:"+e.getMessage();
        }
    }

    public String insert(String index, String type, String jsondata) {
        return null;
    }

    public String update(String index, String type, String jsondata) {
        return null;
    }


    @Test
    public void testUpdateFromMap(){
        try{
            Connection connection = driverManagerDataSource.getConnection();
            String sql = " select * from cpu_biprice where id = 1623998 ";
            ResultSet rs = connection.createStatement().executeQuery(sql);
            Map<String,Object> objectMap = new HashMap<String,Object>();
            while (rs.next()){
                for(String key :ES_FIELD_NAMES){
                    String object = rs.getString(key);
                    if(StringUtils.isNotBlank(object)){
                        if(ES_JSON_KEYMAP.containsKey(key)){
                            JSONObject object1 = JSONObject.parseObject(rs.getString(key));
                            objectMap.put(key,object1);
                        }else if(ES_DOUBLE_KEYMAP.containsKey(key)){
                            objectMap.put(key,rs.getDouble(key));
                        }else{
                            objectMap.put(key,rs.getString(key));
                        }
                    }
                }
            }
            this.updateFromMap("biprice4","price",objectMap);
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }
    public String updateFromMap(String index, String type, Map<String, Object> data) {
        try{
            Client client = elasticsearchTemplate.getClient();
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index)
                    .type(type)
                    .id(String.valueOf(data.get("id")))
                    .doc(data);
            client.update(updateRequest).actionGet();

            //或者使用下面方式（效果一样）
		/*XContentBuilder jsonBuilder =XContentFactory.jsonBuilder();

		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(indexName)
		.type(indexType)
		.id(id)
		.doc(jsonBuilder
				.startObject()
					.field("title").value("XContentBuilder")
				.endObject());

		transportClient.update(updateRequest).actionGet();*/
        }catch (Exception e){
            logger.error(e.getMessage());
            return "修改失败!详细日志:"+e.getMessage();
        }
        return "修改成功!";
    }

    @Test
    public void testDeleteById(){
        this.deleteById("biprice4","price","1623998");
    }

    public String deleteById(String index, String type, String jsondata) {
        try{
            Client client = elasticsearchTemplate.getClient();
            DeleteResponse response = client.prepareDelete(index,type,jsondata).get();
        }catch (Exception e){
            return  "删除失败!详细日志:"+e.getMessage();
        }
        return  "删除成功!";
    }

    public String deleteById(String index, String type, Map<String, Object> map) {
        try{
            Client client = elasticsearchTemplate.getClient();
            DeleteResponse response = client.prepareDelete(index,type,String.valueOf(map.get("vsrc_mark"))).get();
        }catch (Exception e){
            return  "删除失败!详细日志:"+e.getMessage();
        }
        return  "删除成功!";
    }

    public String bulk(String index, String type, List<Map<String, Object>> datas) {
        Client client = elasticsearchTemplate.getClient();

        return  "删除成功!";
    }

    public String delete(String index, String type, String jsondata) {
//        BulkByScrollResponse response =
//                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//                        .filter(QueryBuilders.matchQuery("gender", "male")) //查
//        询条件
//                .source("persons") //index(索引名)
//                .get(); //执行
//        long deleted = response.getDeleted(); //删除文档的数量



//        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//                .filter(QueryBuilders.matchQuery("gender", "male"))
//        //查询
//                .source("persons") //index(索引名)
//                .execute(new ActionListener<BulkByScrollResponse>() {
//                    //回调监听
//                    @Override
//                    public void onResponse(BulkByScrollResponse response) {
//                        long deleted = response.getDeleted(); //删除文档的数
//                        量
//                    }
//                    @Override
//                    public void onFailure(Exception e) {
//// Handle the exception
//                    }
//                });

        return null;
    }

    @Test
    public void deleteByQueryFromTemplate(){
        Connection conn = null;
        try{
            Client client = elasticsearchTemplate.getClient();
            String index = "biprice4";
            String type = "price" ;
            String templateCode = "queryall";
            String jsondata = "" ;
            conn = driverManagerDataSource.getConnection();
            SearchResponse sr = this.getSearchResponseByTemplate(index,type,templateCode,jsondata,conn);
            List<String> idList = new ArrayList<String>();
            if(sr.getHits().getHits().length>0){
                for(SearchHit searchHit:sr.getHits().getHits()){
                    JSONObject jsonObject =  JSONObject.parseObject(searchHit.getSourceAsString());
                    idList.add(searchHit.id());
                }
            }
            if(idList.size()>0){
                BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    }
                })
                        .setBulkActions(10)         //每次10个请求
                        // .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))  //拆成5mb一块
                        // .setConcurrentRequests(1) ////设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着允许1并发请求。
                        // .setBackoffPolicy( BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))   //设置自定义重复请求机制，最开始等待100毫秒，之后成倍更加，重试3次，当一次或多次重复请求失败后因为计算资源不够抛出 EsRejectedExecutionException 异常，可以通过BackoffPolicy.noBackoff()方法关闭重试机制
                        .setConcurrentRequests(0)   //1为异步执行
                        .build();

                // Add your requests
                //        bulkProcessor.add(/* Your requests */);
                //bulkProcessor.add(new IndexRequest("twitter", "tweet", "1").sour
                //bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));

                for(String key :idList){
                    bulkProcessor.add(new DeleteRequest(index,type,key));
                }
                // Flush any remaining requests
                bulkProcessor.flush();
                // Or close the bulkProcessor if you don't need it anymore
                bulkProcessor.close();
                // Refresh your indices
                client.admin().indices().prepareRefresh().get();
                // Now you can start searching!
                client.prepareSearch().get();
            }
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }


    private SearchResponse getSearchResponseByTemplate(String index,String type,String templateCode,String jsondata, Connection conn ) throws Exception {
        JSONObject queryParam = JSONObject.parseObject(jsondata);
        Map<String, String> param = new HashMap<String, String>();
        Map<String, Object> map = queryParam == null ? new HashMap<String, Object>() : queryParam.getInnerMap();
        for(String key:map.keySet()){
            param.put(key,String.valueOf(map.get(key)));
        }
        //整理分页信息
        String pageIndexString = param.get(PAGE_PAGEINDEX);
        String pageSizeString = param.get(PAGE_PAGESIZE);
        int pageIndex = 0 ;
        int pageSize = 10 ;
        long  totalCount = 0 ;
        long  totalPage = 0 ;
        long fromIndex = 0 ;
        if(StringUtils.isNotBlank(pageIndexString)){
            pageIndex = Integer.valueOf(pageIndexString).intValue();
        }
        if(StringUtils.isNotBlank(pageSizeString)){
            pageSize = Integer.valueOf(pageSizeString).intValue();
        }
        if(pageSize==0){
            throw new RuntimeException("每页大小不能为0!");
        }
        fromIndex = pageIndex * pageSize ;
        param.put(PAGE_FROMINDEX,String.valueOf(fromIndex));
        conn = driverManagerDataSource.getConnection();
        String queryTemplateSql = " select * from esconfig_template t left join  esconfig_template_param tp  on t.templatecode = tp.templatecode  where  t.templatecode = '"+templateCode+"' ";
        ResultSet resultSet = conn.createStatement().executeQuery(queryTemplateSql);
        String template = null;
        Map<String,String> paramMap = new HashMap<String,String>();
        while (resultSet.next()){
            if(StringUtils.isBlank(template)){
                template = resultSet.getString("template_script");
            }
            String paramKey = resultSet.getString("paramcode");
            if(StringUtils.isNotBlank(paramKey)){
                paramMap.put(paramKey,resultSet.getString("param_script"));
            }
        }
        if(StringUtils.isBlank(template)){
            throw new RuntimeException("没有配置Es查询模板!");
        }
        if(paramMap.size()>0){
            for(String key:paramMap.keySet()){
                if(param.containsKey(key)){
                    String value = param.get(key);
                    String paramScript = paramMap.get(key);
                    paramScript = paramScript.replace("{{"+key+"}}",value);
                    template = template.replace("##"+key+"##",paramScript);
                }else{
                    template = template.replace("##"+key+"##","");
                }
            }
        }
        //
        Client client = elasticsearchTemplate.getClient();
        SearchResponse sr = client.prepareSearch(index).setTypes(type)
                .setExtraSource(template)
//                    .setTemplateType(ScriptService.ScriptType.INLINE)
//                    .setTemplateParams(map)
                .get();
//            List<String> list =new ArrayList<String>();
        return sr ;
    }
}
