import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESTemplateTest extends SpringTestCase {

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    DriverManagerDataSource driverManagerDataSource;


    @Test
    public void selectByTemplate(){
        try{
            Client client = elasticsearchTemplate.getClient();

            String templateString6 = "{ \"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [ { \"term\": { \"vpurchase_enterprise_id.raw\": \"1029\" }} ,{\"terms\": { \"vmaterial_erp_id.raw\": [\"1001E1100000000058PW\"] }} ] } }, \"boost\": 1.2 } } }";
            String templateString7 = "{ \"query\": { \"prefix\": { \"jpurchase.vpurchase_orginnercode.raw\": { \"value\": \"I8ZX\" } } } }";

            String templateScript8 = "{ \"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [ { \"term\": { \"vpurchase_enterprise_id\": \"1029\" }} ,{\"terms\": { \"vmaterial_id\": [2267921,2267868] }} ] } }, \"boost\": 1.2 } } }";
            String templateScript9 = "{ \"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [ { \"term\": { \"vsupply_name.raw\": \"唐山达能自动化设备有限公司\" }} ] } }, \"boost\": 1.2 } } }";


            String templateString10 = "{ \"from\": 0,\"size\": 10,\"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [ { \"term\": { \"vpurchase_enterprise_id.raw\": \"1029\" }}  ] } }, \"boost\": 1.2 } } }";

            String templateString11 = "{ \"from\": 0,\"size\": 10,\"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [ { \"term\": { \"vpurchase_erp_name.raw\": \"广州酒家集团股份有限公司中央厨房分公司\" }}  ] } }, \"boost\": 1.2 } } }";

            String templateString12 ="{\"from\": 0,\"size\": 100,\"query\": {\"range\" : { \"dbilldate.raw\" : {\"gte\" : \"2017-01-06 15:28:39\",\"lte\" : \"2017-01-06 15:54:45\",\"boost\" : 2.0}}},\"sort\":[{\"dbilldate.raw\":{\"order\":\"desc\"}}]}";

            String templateString13 ="{ \"from\": 0,\"size\": 100,\"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [  {\"range\" : { \"dbilldate.raw\" : {\"gte\" : \"2017-01-06 15:28:39\",\"lte\" : \"2017-01-06 15:54:45\",\"boost\" : 2.0}}}  ] } }, \"boost\": 1.2 } }}";

            String templateString14 = "{ \"from\": 0,\"size\": 100,\"query\": { \"constant_score\": { \"filter\": { \"bool\": { \"must\": [  {\"range\" : { \"dbilldate.raw\" : {\"gte\" : \"2017-01-06 15:28:39\",\"lte\" : \"2017-01-06 15:54:45\",\"boost\" : 2.0}}},{\"terms\": { \"jmaterial.vmaterial_id.raw\": [\"1001E1100000000055YZ\"] }}  ] } }, \"boost\": 1.2 } }}";

            String templateString15 = "{ \"size\":0,\"aggs\": { \"single_avg_price\": { \"terms\": { \"field\": \"vtenant_id\" } } } }";

            Map<String,Object> map = new HashMap<String,Object>();
            map.put("vsupply_name_raw","唐山信誉达五交化有限公司1");
//            map.put("vmaterial_id",new String[]{"3956727","3956769"});
//            map.put("vmaterial_id","3956727,3956769");
            map.put("vadd_type","ycpricedecision");
            map.put("vpurchase_enterprise_id",1029);
            map.put("vpurchase_org_id",1029);
//            client.preparePutIndexedScript("mustache","ycprice_tmp2",templateScript12 ).get();
//            SearchResponse sr = client.prepareSearch()
//                    .setTemplateName("ycprice_tmp2")
//                    .setTemplateType(ScriptService.ScriptType.INDEXED)
//                    .setTemplateParams(map)
//                    .get();
            SearchResponse sr = client.prepareSearch("yc-biprice1").setTypes("price")
                    .setExtraSource(templateString15)
//                    .setTemplateType(ScriptService.ScriptType.INLINE)
//                    .setTemplateParams(map)
                    .get();
            System.out.println("----------------------->>>>>>>>");
            List<String> list =new ArrayList<String>();
            JSONArray jsonArray = new JSONArray();
            if(sr.getHits().getHits().length>0){
                for(SearchHit searchHit:sr.getHits().getHits()){
                    JSONObject jsonObject =  JSONObject.parseObject(searchHit.getSourceAsString());
                    jsonArray.add(jsonObject);
                    list.add(searchHit.getSourceAsString());
                    System.out.println(searchHit.getSourceAsString());
                }
            }
            System.out.println("----------------------->>>>>>>>");
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }

    @Test
    public void insert() throws Exception{
        try{
            Client client = elasticsearchTemplate.getClient();
//            for (int i = 0;i<5;i++) {
//                client.prepareIndex("biprice","his").setSource(
//                        XContentFactory.jsonBuilder().startObject()
//                                .field("tenantid","tenantid"+i)
//                                .field("enterpriseid",100+i)
//                                .field("nprice",100+i)
//                                .field("nnum",20+i)
//                                .field("vunit","个")
//                                .field("vsupplyname","青岛公司"+i)
//                                .endObject()
//                ).get();
//            }
            SearchResponse searchresponse =
                    client.prepareSearch("biprice").setTypes("his").setQuery(
                            QueryBuilders.matchAllQuery()
                    ).get();
            System.out.println("-->>>>>>>>-----------------------");
            if(searchresponse.getHits().getHits().length>0){
                for(SearchHit searchHit:searchresponse.getHits().getHits()){
                    System.out.println("======="+searchHit.getSourceAsString());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }

    @Test
    public void querytest(){
        try{
            Client client =elasticsearchTemplate.getClient();
            BoolQueryBuilder bqb = QueryBuilders.boolQuery();
            bqb.must(QueryBuilders.termQuery("vtenant_id","sjwks31p"));
//            bqb.must(QueryBuilders.boolQuery()
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL_WX_APP))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL_ALI))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL_WX_JS))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL_APP))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_TL_WX_H5))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_WX_GWORLD))
//                    .should(QueryBuilders.matchQuery("payPlatform", SuperAppConstant.PAY_PLATFORM_ALI_GWORLD))
//            );
            SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(bqb).withIndices("yc-biprice").withTypes("price")
                    .withSearchType(SearchType.DEFAULT).build();
            Object o = elasticsearchTemplate.queryForList(searchQuery,null);
            Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
                public Aggregations extract(SearchResponse response) {
                    return response.getAggregations();
                }
            });
            Sum _sum = aggregations.get("tpPrice");
            if(_sum != null){
                logger.info("sum="+_sum.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }

    @Test
    public void queryI(){
        Client client = elasticsearchTemplate.getClient();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("jsupply.jsupply_suppplyid",283421);
// Search
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("yc-biprice1");
        searchRequestBuilder.setTypes("price");
        searchRequestBuilder.setQuery(termQueryBuilder);
// 执行
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        System.out.println("-->>>>>>>>-----------------------");
        if (searchResponse.getHits().getHits().length>0){
            for(SearchHit searchHit :searchResponse.getHits().getHits()){
                System.out.println(searchHit.getSourceAsString());
            }
        }

        System.out.println("-->>>>>>>>-----------------------");
    }

    @Test
    public void testInitAllPrice(){
        initAllPrice(null,"yc-biprice1","price");
    }


    public String initAllPrice(String tenantid,String index,String type) {
        Connection connection = null;
        Statement statement = null ;
        long starttime = System.currentTimeMillis();
        long endtime = 0 ;
        try{
//            Driver driver = (Driver) Class.forName(driverName).newInstance();
//            DriverManager.registerDriver(driver);
//            connection  = DriverManager.getConnection(dburl,username,password);
            connection = driverManagerDataSource.getConnection();
            statement = connection.createStatement();
            this.insertBy500(connection,statement,tenantid,index,type);
            endtime = System.currentTimeMillis();
            System.out.println("--------------------总耗时:"+(endtime-starttime)+"-------------------------");
        }catch (Exception e){
            logger.error(e.getMessage());
            return "初始化索引数据失败!";
        }finally {
            try {
                if(connection!=null){
                    connection.close();
                }
                if(statement!=null){
                    statement.close();
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
                return "初始化索引时释放资源数据失败!";
            }
        }
        return "同步完成,共耗时:"+(endtime-starttime);
    }

    public void insertBy500(Connection connection,Statement statement,String tenantid,String index,String type)throws Exception {
        if(StringUtils.isBlank(index)){
            index = "yc-biprice1";
        }
        if(StringUtils.isBlank(type)){
            type = "price";
        }
        int num_all = 0;
//        String sql = " select count(1) as num from cpu_biprice ";
        String sql = " select count(1) as num from cpu_biprice  ";
        if(StringUtils.isNotBlank(tenantid)){
            sql+= " where vtenant_id = '"+tenantid+"' ";
        }
        ResultSet countRs = statement.executeQuery(sql);
        if (countRs.next()){
            num_all = countRs.getInt("num");
        }
        if(num_all>0){
            int countOnce = 10000;
            for (int num = 0; num * countOnce < num_all; num++) {
                StringBuffer dataSql = new StringBuffer();
                //            dataSql.append(" select * from cpu_biprice where vpurchase_erp_id = '1013' and vpurchase_code is not null ");
//                dataSql.append(" select * from cpu_biprice where vmaterial_id is not null ");
                dataSql.append(" select * from cpu_biprice  ");
                dataSql.append(" limit ").append(countOnce).append(" offset ").append((num) * countOnce);
                long stime1 = System.currentTimeMillis();
                ResultSet resultSet = statement.executeQuery(dataSql.toString());
                long etime1 = System.currentTimeMillis();
                System.out.println("----第"+num+"次查询的时间:"+(etime1-stime1));
                long stime2 = System.currentTimeMillis();
                BulkRequestBuilder bulkRequestBuilder = elasticsearchTemplate.getClient().prepareBulk();
                while (resultSet.next()) {
//                    bulkRequestBuilder.add(elasticsearchTemplate.getClient().prepareIndex(index,type, resultSet.getString("vsrc_mark")).setSource(
//                            XContentFactory.jsonBuilder().startObject()
//                                    .field("id", resultSet.getLong("id"))
//                                    .field("vtenant_id", resultSet.getString("vtenant_id"))
//                                    .field("vpurchase_code", resultSet.getString("vpurchase_code"))
//                                    .field("vpurchase_enterprise_id", resultSet.getString("vpurchase_enterprise_id"))
//                                    .field("vpurchase_erp_name", resultSet.getString("vpurchase_erp_name"))
//                                    .field("vpurchase_org_id", resultSet.getString("vpurchase_org_id"))
//                                    .field("vsupply_name", resultSet.getString("vsupply_name"))
//                                    .field("vsupply_erp_id", resultSet.getString("vsupply_erp_id"))
//                                    .field("vsupply_enterprise_id", resultSet.getString("vsupply_enterprise_id"))
//                                    .field("vsupply_tenant_id", resultSet.getString("vsupply_tenant_id"))
//                                    .field("vsupply_prod_id", resultSet.getString("vsupply_prod_id"))
//                                    .field("vsupply_prod_name", resultSet.getString("vsupply_prod_name"))
//                                    .field("vsupply_prod_code", resultSet.getString("vsupply_prod_code"))
//                                    .field("vsupply_code", resultSet.getString("vsupply_code"))
//                                    .field("vsupply", resultSet.getString("vsupply"))
//                                    .field("vmaterial_id", resultSet.getString("vmaterial_id"))
//                                    .field("vmaterial_code", resultSet.getString("vmaterial_code"))
//                                    .field("vmaterial_name", resultSet.getString("vmaterial_name"))
//                                    .field("vmaterial_type", resultSet.getString("vmaterial_type"))
//                                    .field("vmaterial_spec", resultSet.getString("vmaterial_spec"))
//                                    .field("vmaterial", resultSet.getString("vmaterial"))
//                                    .field("vmaterial_erp_id", resultSet.getString("vmaterial_erp_id"))
//                                    .field("vpurchase_name", resultSet.getString("vpurchase_name"))
//                                    .field("vpurchase", resultSet.getString("vpurchase"))
//                                    .field("vpurchase_erp_id", resultSet.getString("vpurchase_erp_id"))
//                                    .field("vpurchase_erp_code", resultSet.getString("vpurchase_erp_code"))
//                                    .field("vpurchase_enterprise_code", resultSet.getString("vpurchase_enterprise_code"))
//                                    .field("vpurchase_enterprise_name", resultSet.getString("vpurchase_enterprise_name"))
//                                    .field("vprice", resultSet.getString("vprice"))
//                                    .field("nprice_notax", resultSet.getDouble("nprice_notax"))
//                                    .field("nmny", resultSet.getDouble("nmny"))
//                                    .field("nmny_notax", resultSet.getDouble("nmny_notax"))
//                                    .field("nsum", resultSet.getDouble("nsum"))
//                                    .field("nprice", resultSet.getDouble("nprice"))
//                                    .field("ntax", resultSet.getDouble("ntax"))
//                                    .field("vunit_name", resultSet.getString("vunit_name"))
//                                    .field("vcurrency", resultSet.getString("vcurrency"))
//                                    .field("vcondition", resultSet.getString("vcondition"))
//                                    .field("vupdate_mark", resultSet.getString("vupdate_mark"))
//                                    .field("vadd_type", resultSet.getString("vadd_type"))
//                                    .field("dbilldate", resultSet.getString("dbilldate"))
//                                    .field("create_time", resultSet.getString("create_time"))
//                                    .field("ts", resultSet.getString("ts"))
//                                    .field("vsrc", resultSet.getString("vsrc"))
//                                    .field("vsrc_mark", resultSet.getString("vsrc_mark"))
//                                    .field("vsrc_system", resultSet.getString("vsrc_system"))
//                                    .field("vsrc_billcode", resultSet.getString("vsrc_billcode"))
//                                    .field("jpurchase", getMapValue(resultSet.getString("jpurchase")))
//                                    .field("jcondition", getMapValue(resultSet.getString("jcondition")))
//                                    .field("jprice", getMapValue(resultSet.getString("jprice")))
//                                    .field("jmaterial", getMapValue(resultSet.getString("jmaterial")))
//                                    .field("jsupply", getMapValue(resultSet.getString("jsupply")))
//                                    .endObject()
//                            )
//                    );


                    XContentBuilder xContentFactory = XContentFactory.jsonBuilder().startObject();
                    int i = 0;
                    for(String key : BiPriceConstant.ES_FIELD_NAMES_MAP.values()){
                        String value =  resultSet.getString(key);
                        if(StringUtils.isNotBlank(value)){
                            i++;
                            if(BiPriceConstant.ES_JSON_KEYMAP.containsKey(key)){
                                xContentFactory = xContentFactory.field(key,JSONObject.parse(value));
                            }else if(BiPriceConstant.ES_DOUBLE_KEYMAP.containsKey(key)){
                                xContentFactory = xContentFactory.field(key,Double.valueOf(value).doubleValue());
                            }else{
                                xContentFactory = xContentFactory.field(key,value);
                            }
                        }
                    }
                    xContentFactory.endObject();
                    String id = resultSet.getString("vsrc_mark");
                    if(i>0){
                        if(StringUtils.isNotBlank(id)){
                            bulkRequestBuilder.add(elasticsearchTemplate.getClient().prepareIndex(index,type,id).setSource(xContentFactory));
                        }else{
                            bulkRequestBuilder.add(elasticsearchTemplate.getClient().prepareIndex(index,type).setSource(xContentFactory));
                        }
                    }
                }
                long stime = System.currentTimeMillis();
                System.out.println("----第"+num+"次转换的时间:"+(stime-stime2));
                if(bulkRequestBuilder!=null&&bulkRequestBuilder.request()!=null&&bulkRequestBuilder.request().numberOfActions()>0){
                    BulkResponse bulkResponse = bulkRequestBuilder.get();
                    if(bulkResponse.hasFailures()){
                        System.out.println("向ES保存数据的时候报错!详细日志:"+bulkResponse.buildFailureMessage());
                        logger.error("向ES保存数据的时候报错!详细日志:"+bulkResponse.buildFailureMessage());
                    }
                }else{
                    break;
                }
                long etime = System.currentTimeMillis();
                System.out.println("----第"+num+"次的时间:"+(etime-stime));
            }
        }
    }

    public Map<String,Object> getMapValue(String value){
        Map<String,Object> map = new HashMap<String,Object>();
        if(StringUtils.isNotBlank(value)){
            JSONObject jsonObject = JSON.parseObject(value);
            if(jsonObject!=null&&jsonObject.size()>0){
                return jsonObject.getInnerMap();
            }
        }
        return map;
    }

    @Test
    public void testSelectTemplate(){
        try{
            String templagtecode = "selectDetailPage";
            String param = "  {\"vpurchase_enterprise_id\":\"1029\", \"pageSize\": 100,\"vpurchaseOrgId\":\"64831\",\"vaddType\":\"ncpoorder\",\"dbilldateMax\":\"2017-01-07 14:01:24\",\"orderFiled\":\"dbilldate\",\"vmaterialId\":\"11,33,55\"}";
            String param2 = "  {\"vpurchase_enterprise_id\":\"1029\",\"vpurchaseOrgId\":\"\"}";
            String param3 = "{\"vpurchase_enterprise_id\":1029,\"vmaterialId\":\"\",\"pageIndex\":0,\"pageSize\":10}";
            String param4 = "{\"vpurchase_enterprise_id\":1029,\"vaddType\":\"ncpoorder\",\"orderFiled\":\"nprice\",\"orderType\":\"asc\",\"pageIndex\":0,\"pageSize\":100}";
            String param5 = "{\"vpurchase_enterprise_id\":1029,\"vmaterialId\":\"\",\"dbilldateMax\":\"2018-11-01 14:22:51\",\"dbilldateMin\":\"2018-10-27 14:31:32\",\"pageIndex\":0,\"pageSize\":10}";
            String result = selectByTemplate(templagtecode,param5,"yc-biprice1","price");
            String d2 = null;
        }catch (Exception e){
            e.printStackTrace();
            String d = null;
        }
    }

    /**
     *
     * 设计目的:针对不同的查询服务,只需要配置下不同的查询模板,并传入不同的查询参数就可以实现服务
     *
     * 查询逻辑:
     * 1\根据模板编码找到模板和模板变量
     * 2\变量替换: 1)如果参数中包含变量,就把变量对应的脚本中的变量用实际值替换,然后把变量对应的脚本替换到模板中
     *             2)如果参数中不包含变量,就把模板中该变量的位置替换为空
     * 3\执行替换后的脚本,得到数据
     * 4\返回
     * @param templateCode
     * @param jsondata
     * @return
     */
    public String selectByTemplate(String templateCode,String jsondata) {
        Connection conn = null;
        try{
            JSONObject queryParam = JSONObject.parseObject(jsondata);
//            Map<String, String> paramOld = new HashMap<String, String>();
            Map<String, String> param = new HashMap<String, String>();
            Map<String, String> paramValueTypeMap = new HashMap<String, String>();
            Map<String, Object> map = queryParam.getInnerMap();
            if(null!=map&&map.size()>0){
//                for(String key:map.keySet()){
//                    paramOld.put(key,String.valueOf(map.get(key)));
//                }
                conn = driverManagerDataSource.getConnection();
                String paramMappingSql = " select * from esconfig_template_param_mapping  where  templatecode = '"+templateCode+"' ";
                ResultSet paramMappingResultSet = conn.createStatement().executeQuery(paramMappingSql);
                while (paramMappingResultSet.next()){
                    String paramKey = paramMappingResultSet.getString("paramcode");
                    String paramKeyReally = paramMappingResultSet.getString("paramcode_really");
                    if(StringUtils.isNotBlank(paramKey) && StringUtils.isNotBlank(paramKeyReally) && map.containsKey(paramKey)){
                        param.put(paramKeyReally,String.valueOf(map.get(paramKey)));
                    }
                }
            }
            //整理分页信息
            String pageIndexString = param.get(BiPriceConstant.PAGE_PAGEINDEX);
            String pageSizeString = param.get(BiPriceConstant.PAGE_PAGESIZE);
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
            param.put(BiPriceConstant.PAGE_FROMINDEX,String.valueOf(fromIndex));
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
                    String paramValueType = resultSet.getString("valuetype");
                    if(StringUtils.isNotBlank(paramValueType)){
                        paramValueTypeMap.put(paramKey,paramValueType);
                    }
                }
            }
            if(StringUtils.isBlank(template)){
                throw new RuntimeException("没有配置Es查询模板!");
            }
            if(paramMap.size()>0){
                for(String key:paramMap.keySet()){
                    if(param.containsKey(key)&&StringUtils.isNotBlank(param.get(key))){
                        String value = param.get(key);
                        String paramScript = paramMap.get(key);
                        if(paramValueTypeMap.containsKey(key)&&StringUtils.isNotBlank(paramValueTypeMap.get(key))&&paramValueTypeMap.get(key).equals("stringarray")){
                            String[] values = value.split(",");
                            StringBuffer stringBuffer = new StringBuffer();
                            for(int i = 0;i<values.length;i++){
                                stringBuffer.append("\"").append(values[i]).append("\"");
                                if(i<values.length-1){
                                    stringBuffer.append(",");
                                }
                            }
                            paramScript = paramScript.replace("{{"+key+"}}",stringBuffer.toString());
                        }else{
                            paramScript = paramScript.replace("{{"+key+"}}",value);
                        }
                        if("orderFiled".equals(key)){
                            if(param.containsKey("orderType")&&StringUtils.isNotBlank(param.get("orderFiled"))){
                                paramScript = paramScript.replace("{{orderFiled}}",param.get("orderFiled"));
                            }else{
                                paramScript = paramScript.replace("{{orderFiled}}","desc");
                            }
                        }
                        template = template.replace("##"+key+"##",paramScript);
                    }else{
                        template = template.replace("##"+key+"##","");
                    }
                }
            }

            Client client = elasticsearchTemplate.getClient();
            SearchResponse sr = client.prepareSearch("yc-biprice1").setTypes("price")
                    .setExtraSource(template)
//                    .setTemplateType(ScriptService.ScriptType.INLINE)
//                    .setTemplateParams(map)
                    .get();
//            List<String> list =new ArrayList<String>();
            JSONArray jsonArray = new JSONArray();
            if(sr.getHits().getHits().length>0){
                totalCount = sr.getHits().getTotalHits();
                if(totalCount>0){
                    totalPage = totalCount / pageSize;
                    long yu = totalCount % pageSize;
                    if(yu>0){
                        totalPage += 1;
                    }
                }
                for(SearchHit searchHit:sr.getHits().getHits()){
                    JSONObject jsonObject =  JSONObject.parseObject(searchHit.getSourceAsString());
                    jsonArray.add(jsonObject);
//                    list.add(searchHit.getSourceAsString());
                    System.out.println(searchHit.getSourceAsString());
                }
            }
            JSONObject data = new JSONObject();
            data.put(BiPriceConstant.RESULT,jsonArray);
            data.put(BiPriceConstant.PAGE_TOTALCOUNT,totalCount);
            data.put(BiPriceConstant.PAGE_TOTALPAGE,totalPage);
            data.put(BiPriceConstant.PAGE_PAGEINDEX,pageIndex);
            data.put(BiPriceConstant.PAGE_PAGESIZE,pageSize);
//            return BIJsonUtils.returnWhenSuccessJsonData("查询成功!",data);
            return "查询成功!";
        }catch (Exception e){
            logger.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }finally {
            try {
                if(conn!=null){
                    conn.close();
                }
            }catch (Exception e){
                logger.error(e.getMessage());
                throw new RuntimeException(e.getMessage());
            }
        }
    }


    public String selectByTemplate(String templateCode,String jsondata,String index ,String type) {
        Connection conn = null;
        try{
            logger.info("查询参数index["+index+"],type["+type+"],templateCode["+templateCode+"]和查询参数数据:"+jsondata);
            JSONObject queryParam = JSONObject.parseObject(jsondata);
//            Map<String, String> paramOld = new HashMap<String, String>();
            Map<String, String> param = new HashMap<String, String>();
            Map<String, String> paramValueTypeMap = new HashMap<String, String>();
            Map<String, Object> map = queryParam.getInnerMap();
            if(null!=map&&map.size()>0){
//                for(String key:map.keySet()){
//                    paramOld.put(key,String.valueOf(map.get(key)));
//                }
                conn = driverManagerDataSource.getConnection();
                String paramMappingSql = " select * from esconfig_template_param_mapping  where  templatecode = '"+templateCode+"' ";

                Map<String,String> templateParamKeyMap = new HashMap<String,String>();
                logger.info("查询模板参数映射的脚本:"+paramMappingSql);
                ResultSet paramMappingResultSet = conn.createStatement().executeQuery(paramMappingSql);
                while (paramMappingResultSet.next()){
                    String paramKey = paramMappingResultSet.getString("paramcode");
                    templateParamKeyMap.put(paramKey,paramKey);
                    String paramKeyReally = paramMappingResultSet.getString("paramcode_really");
                    if(StringUtils.isNotBlank(paramKey) && StringUtils.isNotBlank(paramKeyReally) && map.containsKey(paramKey)){
                        param.put(paramKeyReally,String.valueOf(map.get(paramKey)));
                    }
                }
                //
                for(String key:map.keySet()){
                    if(!templateParamKeyMap.containsKey(key)){
                        param.put(key,String.valueOf(map.get(key)));
                    }
                }
            }
            //整理分页信息
            String pageIndexString = param.get(BiPriceConstant.PAGE_PAGEINDEX);
            String pageSizeString = param.get(BiPriceConstant.PAGE_PAGESIZE);
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
            param.put(BiPriceConstant.PAGE_FROMINDEX,String.valueOf(fromIndex));

            String queryTemplateSql = " select * from esconfig_template t left join  esconfig_template_param tp  on t.templatecode = tp.templatecode  where  t.templatecode = '"+templateCode+"' ";

            logger.info("查询模板和参数的脚本:"+queryTemplateSql);
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
                    String paramValueType = resultSet.getString("valuetype");
                    if(StringUtils.isNotBlank(paramValueType)){
                        paramValueTypeMap.put(paramKey,paramValueType);
                    }
                }
            }
            if(StringUtils.isBlank(template)){
                throw new RuntimeException("没有配置Es查询模板!");
            }
            if(paramMap.size()>0){
                for(String key:paramMap.keySet()){
                    if(param.containsKey(key)&&StringUtils.isNotBlank(param.get(key))){
                        String value = param.get(key);
                        String paramScript = paramMap.get(key);
                        if(paramValueTypeMap.containsKey(key)&&StringUtils.isNotBlank(paramValueTypeMap.get(key))&&paramValueTypeMap.get(key).equals("stringarray")){
                            String[] values = value.split(",");
                            StringBuffer stringBuffer = new StringBuffer();
                            for(int i = 0;i<values.length;i++){
                                stringBuffer.append("\"").append(values[i]).append("\"");
                                if(i<values.length-1){
                                    stringBuffer.append(",");
                                }
                            }
                            paramScript = paramScript.replace("{{"+key+"}}",stringBuffer.toString());
                        }else{
                            paramScript = paramScript.replace("{{"+key+"}}",value);
                        }
                        if("orderFiled".equals(key)){
                            paramScript = paramScript.replace("{{orderFiled}}",param.get("orderFiled"));
                            if(param.containsKey("orderType")&&StringUtils.isNotBlank(param.get("orderType"))){
                                paramScript = paramScript.replace("{{orderType}}",param.get("orderType"));
                            }else{
                                paramScript = paramScript.replace("{{orderType}}","desc");
                            }
                        }
                        template = template.replace("##"+key+"##",paramScript);
                    }else{
                        template = template.replace("##"+key+"##","");
                    }
                }
            }
            logger.info("查询模板脚本为:"+template);
            Client client = elasticsearchTemplate.getClient();
            SearchResponse sr = client.prepareSearch(index).setTypes(type)
                    .setExtraSource(template)
//                    .setTemplateType(ScriptService.ScriptType.INLINE)
//                    .setTemplateParams(map)
                    .get();
//            List<String> list =new ArrayList<String>();
            JSONArray jsonArray = new JSONArray();
            if(sr.getHits().getHits().length>0){
                totalCount = sr.getHits().getTotalHits();
                if(totalCount>0){
                    totalPage = totalCount / pageSize;
                    long yu = totalCount % pageSize;
                    if(yu>0){
                        totalPage += 1;
                    }
                }
                for(SearchHit searchHit:sr.getHits().getHits()){
                    JSONObject jsonObject =  JSONObject.parseObject(searchHit.getSourceAsString());
                    jsonArray.add(jsonObject);
//                    list.add(searchHit.getSourceAsString());
                    System.out.println(searchHit.getSourceAsString());
                }
            }
            JSONObject data = new JSONObject();
            data.put(BiPriceConstant.RESULT,jsonArray);
            data.put(BiPriceConstant.PAGE_TOTALCOUNT,totalCount);
            data.put(BiPriceConstant.PAGE_TOTALPAGE,totalPage);
            data.put(BiPriceConstant.PAGE_PAGEINDEX,pageIndex);
            data.put(BiPriceConstant.PAGE_PAGESIZE,pageSize);
//            logger.info("查询结果:"+JsonUtils.toJson(jsonArray));
//            return BIJsonUtils.returnWhenSuccessJsonData("查询成功!",data);
            return "查询成功!";
        }catch (Exception e){
            logger.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }finally {
            try {
                if(conn!=null){
                    conn.close();
                }
            }catch (Exception e){
                logger.error(e.getMessage());
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
