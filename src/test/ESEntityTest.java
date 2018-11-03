import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESEntityTest extends SpringTestCase {

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    DriverManagerDataSource driverManagerDataSource;

String data = "[{\"actualinvalidate\":null,\"actualvalidate\":1540828800000,\"addressName\":null,\"advanceRatio\":0E-8,\"arrivalBillNum\":null,\"attachmentIds\":null,\"bankAccount\":null,\"bidder\":null,\"billCode_\":null,\"billType_\":null,\"billno\":\"zxftest2\",\"billstatus\":\"20\",\"billstatusName\":null,\"billtype\":\"2\",\"buyOfferId\":null,\"buyofferId\":null,\"changingAction\":null,\"changingCommittedDate\":1540903236000,\"changingCommittedReason\":null,\"changingCommitterName\":\"ycwyg65,名称\",\"changingConfirmedDate\":1540903264000,\"changingConfirmerName\":\"ycwygsupply02用户\",\"changingInfo\":\"{\\\"actualinvalidate\\\":\\\"\\\",\\\"deliveryAddress\\\":null,\\\"execType\\\":\\\"0\\\",\\\"invoiceAgreement\\\":null,\\\"memo\\\":null,\\\"outerCTBillCode\\\":null,\\\"payAgreement\\\":null,\\\"phone\\\":null,\\\"projectAddress\\\":null,\\\"receiver\\\":null,\\\"settleType\\\":null,\\\"shelfRatio\\\":null,\\\"shelfPeriod\\\":null,\\\"subject\\\":\\\"zxftest2\\\",\\\"taxMoney\\\":5382,\\\"newMaterialList\\\":[],\\\"newTermList\\\":[],\\\"newPayTermList\\\":[]}\",\"changingRejectedReason\":null,\"changingStatus\":\"20\",\"changingVersion\":\"2\",\"contactNumber\":null,\"contractExpList\":null,\"contractMaterialList\":[{\"accomplishedQuantity\":234.00000000,\"addressName\":null,\"benchmarkType\":null,\"brand\":null,\"changingAction\":null,\"changingInfo\":null,\"city_id\":null,\"contractId\":555601,\"createTime\":null,\"creator\":null,\"deliveryAddress\":null,\"district_id\":null,\"dr\":0,\"eRPProjectId\":null,\"eRPProjectName\":null,\"enableExecNum\":null,\"enterpriseId\":1029,\"enterpriseName\":null,\"erpCtCode\":null,\"erpCtbid\":null,\"erpCtid\":null,\"erpMaterialId\":null,\"expectArriveDate\":null,\"extFields\":null,\"field1\":null,\"field10\":null,\"field11\":null,\"field12\":null,\"field13\":null,\"field14\":null,\"field15\":null,\"field16\":null,\"field17\":null,\"field18\":null,\"field19\":null,\"field2\":null,\"field20\":null,\"field3\":null,\"field4\":null,\"field5\":null,\"field6\":null,\"field7\":null,\"field8\":null,\"field9\":null,\"firstBillBid\":null,\"firstBillId\":null,\"firstBillType\":null,\"firstBillcode\":null,\"id\":1315681,\"inputMemo\":null,\"inputPlanarriveDate\":null,\"inputQuantity\":null,\"materialClassCode\":\"40101\",\"materialClassId\":75407,\"materialClassName\":\"冻猪肉类\",\"materialCode\":\"00001\",\"materialDesc\":null,\"materialId\":4437890,\"materialName\":\"狗头\",\"memo\":null,\"model\":\"联想\",\"modifiedtime\":null,\"modifier\":null,\"money\":5225.24270000,\"num\":234.00000000,\"overExecRate\":null,\"payTypesSelectValue\":null,\"planArriveDate\":null,\"price\":22.33010000,\"priceValidateDate\":null,\"pritemid\":null,\"productId\":null,\"productName\":null,\"province_id\":null,\"receiveOrgId\":null,\"receiveOrgName\":null,\"receivePersonId\":null,\"receivePersonName\":null,\"receivePersonTel\":null,\"receiveorgcode\":null,\"receivepersoncode\":null,\"refNo\":null,\"remainQuantity\":0E-8,\"reqId\":null,\"reqOrgId\":null,\"reqOrgName\":null,\"reqSrcBillCode\":null,\"reqSrcRowNo\":null,\"rowNum\":1,\"rowStatus\":null,\"settlementType\":null,\"shelfLife\":null,\"skuId\":null,\"skuUrl\":null,\"sortItemMap\":null,\"spec\":\"32寸\",\"srcBillBid\":null,\"srcBillId\":null,\"srcBillType\":null,\"srcBillcode\":null,\"suppProductName\":null,\"sysVersion\":null,\"taxFullMoney\":null,\"taxFullPrice\":null,\"taxMoney\":5382.00000000,\"taxPartialMoney\":null,\"taxPartialPrice\":null,\"taxPrice\":23.00000000,\"taxlessFullMoney\":null,\"taxlessFullPrice\":null,\"taxlessPartialMoney\":null,\"taxlessPartialPrice\":null,\"taxrate\":3,\"ts\":1540903226000,\"unit\":\"台\",\"unitid\":null}],\"contractPayTermList\":[],\"contractScopeList\":null,\"contractTermList\":[],\"controlType\":\"0\",\"createTime\":1540903226000,\"creator\":\"2113\",\"ctControlName\":null,\"ctPurContentName\":null,\"currencyName\":null,\"deliveryAddress\":null,\"deptId\":64667,\"deptName\":\"总经办\",\"dr\":0,\"eRPProjectId\":null,\"eRPProjectName\":null,\"ectBillCode\":null,\"ectFilePath\":null,\"ectId\":null,\"ectTemplateCode\":null,\"ectTemplateId\":null,\"ectTemplateName\":null,\"enterPriseId_\":1029,\"enterpriseId\":1029,\"enterpriseName\":\"云采测试专用forNC65\",\"erpCtCode\":null,\"erpCtid\":null,\"erpOrgCode\":null,\"erpPersionCode\":null,\"erpProductVersion\":null,\"erpSupId\":null,\"erpUserCode\":null,\"execName\":null,\"execType\":\"0\",\"extFields\":null,\"fbillstatus\":null,\"field1\":null,\"field10\":null,\"field11\":null,\"field12\":null,\"field13\":null,\"field14\":null,\"field15\":null,\"field16\":null,\"field17\":null,\"field18\":null,\"field19\":null,\"field2\":null,\"field20\":null,\"field3\":null,\"field4\":null,\"field5\":null,\"field6\":null,\"field7\":null,\"field8\":null,\"field9\":null,\"furtureTemplate\":\"0\",\"hasWorkFlow\":false,\"id\":555601,\"index\":null,\"index_\":null,\"invoiceAgreement\":null,\"inwhMoney\":null,\"inwhnum\":null,\"isMaterialClass\":\"0\",\"materMess\":null,\"materialInfo\":null,\"memo\":null,\"modifiedtime\":1540903226000,\"modifier\":\"ycwyg65,名称\",\"money\":5225.24270000,\"numberTotal\":null,\"offerType\":\"1\",\"openBank\":null,\"orderBillNum\":null,\"orgId\":64892,\"orgName\":\"广州酒家集团利口福营销有限公司\",\"orgScope\":false,\"outerCTBillCode\":null,\"parentContractId\":null,\"parentContractName\":null,\"parentContractno\":null,\"payAgreement\":null,\"payTypes\":null,\"phone\":null,\"pricedecisionId\":null,\"projectAddress\":null,\"projectCode\":null,\"projectDocId\":null,\"projectId\":null,\"projectName\":null,\"purContentType\":null,\"purPersonId\":113134,\"purPersonMobile\":null,\"purPersonName\":\"陈少宾\",\"purPersonTel\":null,\"purUserId\":2113,\"purchaseMess\":null,\"purchasePer\":null,\"quotaSupplyMny\":null,\"quotaSupplys\":0,\"reason\":null,\"receiveAddress\":null,\"receiveArea\":null,\"receiver\":null,\"saleContractId\":null,\"senderLoginName\":null,\"settleName\":null,\"settleType\":null,\"shelfPeriod\":null,\"shelfRatio\":null,\"sigMoney\":null,\"signAddress\":null,\"signatureEffect\":false,\"signatureFailMessage\":null,\"signatureStatus\":null,\"signum\":null,\"sortItemMap\":null,\"sourceType\":\"1\",\"subject\":\"zxftest2\",\"subscribedate\":1540828800000,\"sumMoney\":null,\"sumNoSigMoney\":null,\"sumNoSigNumber\":null,\"sumSigNumber\":null,\"sumSignumMoney\":null,\"supBank\":null,\"supBankAccount\":null,\"supCorpAddress\":null,\"supEnterpriseId\":336,\"supEnterpriseName\":\"ycwygsupply02从化市螯头瑞利种养殖场\",\"supLegalBody\":null,\"supPersionId\":null,\"supPersionName\":null,\"supPersonMobile\":null,\"supPersonTel\":null,\"supTel\":null,\"suppAddress\":null,\"supplyDelivery\":null,\"supplyPayment\":null,\"supplycode\":null,\"supplyid\":null,\"supplyname\":null,\"sysVersion\":null,\"taxMoney\":5382,\"taxNumber\":null,\"taxlessFullMoney\":0E-8,\"taxlessPartialMoney\":0E-8,\"termManualAddEnable\":null,\"termTemplateId\":null,\"thisEnterpriseHasERP\":false,\"totalFullMoney\":0E-8,\"totalPartialMoney\":0E-8,\"totalPayMny\":null,\"totalnum\":234.00000000,\"transtypeCode\":null,\"transtypeId\":null,\"transtypeName\":null,\"transtypeSource\":null,\"ts\":1540903226000,\"vBuyOfferBillCode\":null,\"workPeriod\":null}]";

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
    public void saveTest(){
        String data ="[{\"vtenantId\":\"czw4u4qb\",\"vpurchaseEnterpriseId\":\"1029\",\"vpurchaseEnterpriseCode\":\"\",\"vpurchaseEnterpriseName\":\"云采测试专用forNC65\",\"vpurchaseOrgId\":\"64892\",\"vpurchaseErpName\":\"广州酒家集团利口福营销有限公司\",\"vsupplyCode\":\"6010002\",\"vsupplyName\":\"ycwygsupply02从化市螯头瑞利种养殖场\",\"vmaterialId\":\"3957465\",\"vmaterialCode\":\"\",\"vmaterialName\":\"不锈钢球形双丝头\",\"nsum\":11,\"nprice\":11,\"vsrcMark\":\"czw4u4qb@@@YC_CT_2_BI@@@1315524\",\"dbilldate\":\"1540656000000\",\"vupdateMark\":\"czw4u4qb@#$20181029202714%&czw4u4qb@@@YC_CT_2_BI@@@1315524\",\"vmaterialSpec\":\"\",\"vmaterialType\":\"材质304    Ф15*1.5MM\",\"vunitName\":\"个\",\"ntax\":11,\"vsrcSystem\":\"YC\",\"vaddType\":\"yccontract\",\"vsrcBillCode\":\"LKZJ2018@@0016\",\"jsupply\":{\"vsupply_sup_enterpriseid\":336,\"vsupply_supplierId\":44609},\"jprice\":{\"vprice_offerType\":\"1\",\"vprice_bid\":1315524,\"vprice_hid\":555596,\"vprice_payTypes\":\"\"}}]";
        String data2 = "[{\"vtenantId\":\"czw4u4qb\",\"vpurchaseEnterpriseId\":\"1029\",\"vpurchaseEnterpriseCode\":\"\",\"vpurchaseEnterpriseName\":\"云采测试专用forNC65\",\"vpurchaseOrgId\":\"64892\",\"vpurchaseErpName\":\"广州酒家集团利口福营销有限公司\",\"vsupplyCode\":\"6010002\",\"vsupplyName\":\"ycwygsupply02从化市螯头瑞利种养殖场\",\"vmaterialId\":\"3957465\",\"vmaterialCode\":\"\",\"vmaterialName\":\"不锈钢球形双丝头\",\"nsum\":11,\"nprice\":11,\"vsrcMark\":\"czw4u4qb@@@YC_CT_2_BI@@@1315524\",\"dbilldate\":\"2018-10-28 00:00:00\",\"vupdateMark\":\"czw4u4qb@#$20181030021625%&czw4u4qb@@@YC_CT_2_BI@@@1315524\",\"vmaterialSpec\":\"\",\"vmaterialType\":\"材质304    Ф15*1.5MM\",\"vunitName\":\"个\",\"ntax\":11,\"vsrcSystem\":\"YC\",\"vaddType\":\"yccontract\",\"vsrcBillCode\":\"LKZJ2018@@0016\",\"jsupply\":{\"vsupply_sup_enterpriseid\":336,\"vsupply_supplierId\":44609},\"jprice\":{\"vprice_offerType\":\"1\",\"vprice_bid\":1315524,\"vprice_invalidate\":\"\",\"vprice_hid\":555596,\"vprice_payTypes\":\"\"}}]";
        this.batchInsertEntField("yc-biprice1","price",data);
    }

    public String batchInsertEntField(String index, String type, String jsondata) {
        try{
            index = getIndex(index);
            type = getIndexType(type);
            if(StringUtils.isNotBlank(jsondata)){
                List<Map<String, String>> datas = new ArrayList<Map<String, String>>();
                JSONArray jsonArray = JSONArray.parseArray(jsondata);
                if(null!=jsonArray&&jsonArray.size()>0){
                    for(int i=0;i<jsonArray.size();i++){
                        Map<String, String> map = new HashMap<String, String>();
                        JSONObject jsonObject = jsonArray.getJSONObject(i);
                        for(String key : BiPriceConstant.ES_FIELD_NAMES_MAP.keySet()){
                            if(jsonObject.containsKey(key)){
                                map.put(BiPriceConstant.ES_FIELD_NAMES_MAP.get(key),jsonObject.getString(key));
                            }
                        }
                        if(map.size()>0){
                            datas.add(map);
                        }
                    }
                }
                if(datas.size()>0){
                    this.batchInsertFromMapDBField(index,type,datas);
                }
            }
//            return BIJsonUtils.returnWhenSuccessJsonData("批量报错成功!",null);
            return "bccg";
        }catch (Exception e){
            logger.info("批量报错报错!"+e.getMessage());
            return "批量报错报错";
        }
    }



    public String batchInsertFromMapDBField(String index, String type, List<Map<String, String>> datas) {
        long stime = System.currentTimeMillis();
        try {
            BulkRequestBuilder bulkRequestBuilder = elasticsearchTemplate.getClient().prepareBulk();
            if (datas!=null&&datas.size()>0){
                int num = 0 ;
                for(Map<String,String> map:datas){
                    if(null!=map){
                        XContentBuilder xContentFactory = XContentFactory.jsonBuilder().startObject();
                        int i = 0;
                        for(String key : BiPriceConstant.ES_FIELD_NAMES_MAP.values()){
                            if(map.containsKey(key)){
                                String value = map.get(key);
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
                        }
                        xContentFactory.endObject();
                        String id = map.get("vsrc_mark");
                        if(i>0){
                            num++;
                            if(StringUtils.isNotBlank(id)){
                                bulkRequestBuilder.add(elasticsearchTemplate.getClient().prepareIndex(index,type,id).setSource(xContentFactory));
                            }else{
                                bulkRequestBuilder.add(elasticsearchTemplate.getClient().prepareIndex(index,type).setSource(xContentFactory));
                            }
                        }
                    }
                }
                if (num>0){
                    if(bulkRequestBuilder!=null&&bulkRequestBuilder.request()!=null&&bulkRequestBuilder.request().numberOfActions()>0){
                        BulkResponse bulkResponse = bulkRequestBuilder.get();
                        if(bulkResponse.hasFailures()){
                            logger.error("保存数据到es的时候发生错误!详细日志:"+bulkResponse.buildFailureMessage());
                        }
                    }
                }
            }
            long etime = System.currentTimeMillis();
            logger.info("批量保存成功!耗时["+(etime-stime)+"]毫秒");
            return "success";
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("批量保存出现IO异常!详细日志:"+e.getMessage());
        }catch (Exception e){
            logger.error(e.getMessage());
            throw new RuntimeException("批量保存报错!详细日志:"+e.getMessage());
        }
    }



    public static String getIndex(String index){
        if(StringUtils.isNotBlank(index)){
            return index;
        }
        return ESConstrant.ES_DEFAULT_INDEXNAME;
    }

    public static String getIndexType(String indexType){
        if(StringUtils.isNotBlank(indexType)){
            return indexType;
        }
        return ESConstrant.ES_DEFAULT_TYPENAME;
    }
}
