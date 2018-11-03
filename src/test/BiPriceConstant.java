
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by fengjqc on 2017/3/23.
 */
public class BiPriceConstant implements Serializable {
    public static final String LOWESTPRICE = "LOWESTPRICE";
    public static final String AVERAGEPRICE = "AVERAGEPRICE";
    public static final String HIGHESTPRICE = "HIGHESTPRICE";
    public static final String LASTPRICE = "LASTPRICE";
    public static final String VSUPPLYNAME = "VSUPPLYNAME";
    public static final String DBILLDATE = "DBILLDATE";



    /**
     *  属性名常量值都要用小写
     */
    //属性前缀，用来判断是属于哪个分类的属性
    public static final String ERP_PREFIX_PURCHASE="vpurchase_";
    public static final String ERP_PREFIX_SUPPLY="vsupply_";
    public static final String ERP_PREFIX_MATERIAL="vmaterial_";
    public static final String ERP_PREFIX_PRICE="vprice_";
    public static final String ERP_PREFIX_CONDITION="vcondition_";


    public static final String Erp_Pk_material="pk_material";
    public static final String Erp_Id="id";
    public static final String Erp_Pk_org="pk_org";
    public static final String Erp_Pk_supplier="pk_supplier";
    public static final String Erp_Dbilldate="dbilldate";
    public static final String Erp_Norigtaxprice="norigtaxprice";

    public static final String Erp_Nsum="nsum";
    public static final String Erp_Nmny="nmny";
    public static final String Erp_Vsrc="vsrc";
    public static final String Erp_VunitName="vunitname";
    public static final String Erp_Vcurrency="vcurrency";
    public static final String Erp_NpriceNoTax="npricenotax";
    public static final String Erp_Ntax="ntax";
    public static final String Erp_NmnyNoTax="nmnynotax";
    public static final String Erp_VsrcSystem="vsrcsystem";
    public static final String Erp_VaddType="vaddtype";
    public static final String Erp_VsrcBillCode="vsrcbillcode";

    public static final String Erp_VpurchaseErpCode="vpurchase_erp_code";
    public static final String Erp_VpurchaseErpName="vpurchase_erp_names";



    //bipriceentiry 实体属性常量
    public static final String BI_DBILLDATE="dbilldate";
    public static final String BI_NPRICE="nprice";
    public static final String BI_VPURCHASEENTERPRISEID="vpurchaseEnterpriseId";
    public static final String BI_VPURCHASEENTERPRISENAME="vpurchaseEnterpriseName";
    public static final String BI_VMATERIALID="vmaterialId";
    public static final String BI_VMATERIALCODE="vmaterialCode";
    public static final String BI_VMATERIALNAME="vmaterialName";
    public static final String BI_VMATERIALSPEC="vmaterialSpec";
    public static final String BI_VMATERIALTYPE="vmaterialType";
    public static final String BI_VSRCSYSTEM="vsrcSystem";
    public static final String BI_VADDTYPE="vaddType";
    public static final String BI_VSRCBILLCODE="vsrcBillCode";
    public static final String BI_VSUPPLYNAME = "vsupplyName";
    public static final String BI_NSUM="nsum";
    public static final String BI_NTAX="ntax";
    public static final String BI_VPURCHASEERPNAME = "vpurchaseErpName";


    public static final String BI_VPURCHASEENTERPRISECODE = "vpurchaseEnterpriseCode";
    public static final String BI_VPURCHASEORGID="vpurchaseOrgId";
    public static final String BI_VPURCHASEERPID="vpurchaseErpId";
    public static final String BI_VPURCHASEERPCODE="vpurchaseErpCode";
    public static final String BI_VPURCHASECODE="vpurchaseCode";
    public static final String BI_VPURCHASENAME="vpurchaseName";
    public static final String BI_VPURCHASE="vpurchase";
    public static final String BI_VSUPPLYTENANTID="vsupplyTenantId";
    public static final String BI_VSUPPLYENTERPRISEID = "vsupplyEnterpriseId";
    public static final String BI_VSUPPLYERPID="vsupplyErpId";
    public static final String BI_VSUPPLYCODE="vsupplyCode";
    public static final String BI_VSUPPLY="vsupply";
    public static final String BI_VMATERIALERPID="vmaterialErpId";
    public static final String BI_VMATERIAL="vmaterial";
    public static final String BI_NMNY="nmny";
    public static final String BI_VCONDITION = "vcondition";
    public static final String BI_VPRICE="vprice";
    public static final String BI_VSRC="vsrc";
    public static final String BI_VSRCMARK = "vsrcMark";
    public static final String BI_TS = "ts";
    public static final String BI_VUPDATEMARK="vupdateMark";
    public static final String BI_VSUPPLYPRODID="vsupplyProdId";
    public static final String BI_VSUPPLYPRODNAME="vsupplyProdName";
    public static final String BI_VSUPPLYPRODCODE="vsupplyProdCode";
    public static final String BI_VUNITNAME="vunitName";
    public static final String BI_VCURRENCY = "vcurrency";
    public static final String BI_NPRICENOTAX="npriceNoTax";
    public static final String BI_NMNYNOTAX="nmnyNoTax";





    //接口查询参数名称
    public static final String param_purchaserId="purchaserId";
    public static final String param_supplierId="supplierId";
    public static final String param_materialId="materialId";

//    static {
//          Set<String> Set_Field = new HashSet<String>();
//          Set_Field.add();
//    }

    //分页信息
    public static final String PAGE_PAGEINDEX = "pageIndex" ;
    public static final String PAGE_PAGESIZE = "pageSize" ;
    public static final String PAGE_TOTALCOUNT = "totalCount" ;
    public static final String PAGE_TOTALPAGE = "totalPage" ;
    public static final String PAGE_FROMINDEX = "fromIndex" ;
    public static final String RESULT = "result" ;



    public static final Map<String,String> ES_FIELD_NAMES_MAP = new HashMap<String, String>();
    static {
        ES_FIELD_NAMES_MAP.put("id","id");

        ES_FIELD_NAMES_MAP.put("vtenantId","vtenant_id");
        ES_FIELD_NAMES_MAP.put("vpurchaseEnterpriseId","vpurchase_enterprise_id");
        ES_FIELD_NAMES_MAP.put("vpurchaseEnterpriseCode","vpurchase_enterprise_code");
        ES_FIELD_NAMES_MAP.put("vpurchaseEnterpriseName","vpurchase_enterprise_name");
        ES_FIELD_NAMES_MAP.put("vpurchaseCode","vpurchase_code");

        ES_FIELD_NAMES_MAP.put("vpurchaseErpId","vpurchase_erp_id");
        ES_FIELD_NAMES_MAP.put("vpurchaseErpName","vpurchase_erp_name");
        ES_FIELD_NAMES_MAP.put("vpurchaseErpCode","vpurchase_erp_code");
        ES_FIELD_NAMES_MAP.put("vpurchaseOrgId","vpurchase_org_id");
        ES_FIELD_NAMES_MAP.put("vpurchaseName","vpurchase_name");

        ES_FIELD_NAMES_MAP.put("vsupplyEnterpriseId","vsupply_enterprise_id");
        ES_FIELD_NAMES_MAP.put("vsupplyTenantId","vsupply_tenant_id");
        ES_FIELD_NAMES_MAP.put("vsupplyErpId","vsupply_erp_id");
        ES_FIELD_NAMES_MAP.put("vsupplyName","vsupply_name");
        ES_FIELD_NAMES_MAP.put("vsupplyCode","vsupply_code");

        ES_FIELD_NAMES_MAP.put("vsupplyProdId","vsupply_prod_id");
        ES_FIELD_NAMES_MAP.put("vsupplyProdName","vsupply_prod_name");
        ES_FIELD_NAMES_MAP.put("vsupplyProdCode","vsupply_prod_code");
        ES_FIELD_NAMES_MAP.put("vmaterialId","vmaterial_id");
        ES_FIELD_NAMES_MAP.put("vmaterialName","vmaterial_name");

        ES_FIELD_NAMES_MAP.put("vmaterialCode","vmaterial_code");
        ES_FIELD_NAMES_MAP.put("vmaterialType","vmaterial_type");
        ES_FIELD_NAMES_MAP.put("vmaterialSpec","vmaterial_spec");
        ES_FIELD_NAMES_MAP.put("vmaterial","vmaterial");
        ES_FIELD_NAMES_MAP.put("vmaterialErpId","vmaterial_erp_id");

        ES_FIELD_NAMES_MAP.put("nprice","nprice");
        ES_FIELD_NAMES_MAP.put("nsum","nsum");
        ES_FIELD_NAMES_MAP.put("ntax","ntax");
        ES_FIELD_NAMES_MAP.put("vprice","vprice");
        ES_FIELD_NAMES_MAP.put("npriceNoTax","nprice_notax");

        ES_FIELD_NAMES_MAP.put("nmny","nmny");
        ES_FIELD_NAMES_MAP.put("nmnyNoTax","nmny_notax");
        ES_FIELD_NAMES_MAP.put("dbilldate","dbilldate");
        ES_FIELD_NAMES_MAP.put("vunitName","vunit_name");
        ES_FIELD_NAMES_MAP.put("vaddType","vadd_type");

        ES_FIELD_NAMES_MAP.put("vsrcMark","vsrc_mark");
        ES_FIELD_NAMES_MAP.put("vsrcSystem","vsrc_system");
        ES_FIELD_NAMES_MAP.put("vsrcBillCode","vsrc_billcode");
        ES_FIELD_NAMES_MAP.put("vsrc","vsrc");
        ES_FIELD_NAMES_MAP.put("create_time","create_time");

        ES_FIELD_NAMES_MAP.put("ts","ts");
        ES_FIELD_NAMES_MAP.put("vupdateMark","vupdate_mark");
        ES_FIELD_NAMES_MAP.put("vcondition","vcondition");
        ES_FIELD_NAMES_MAP.put("vsupply","vsupply");
        ES_FIELD_NAMES_MAP.put("vpurchase","vpurchase");

        ES_FIELD_NAMES_MAP.put("vcurrency","vcurrency");
        ES_FIELD_NAMES_MAP.put("jpurchase","jpurchase");
        ES_FIELD_NAMES_MAP.put("jsupply","jsupply");
        ES_FIELD_NAMES_MAP.put("jcondition","jcondition");
        ES_FIELD_NAMES_MAP.put("jprice","jprice");

        ES_FIELD_NAMES_MAP.put("jmaterial","jmaterial");
    };

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

    public static final Map<String,String> entField2DBFieldMapping = new HashMap<String,String>();


    //查询接口参数定义
    public static final String ENTERPRISEID  = "enterPriseId";


}
