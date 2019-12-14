package com.qf.bigdata.realtime.util.data;

import java.io.Serializable;

public abstract class BasicHelper implements Serializable {

    //是否
    public static final String COMM_TRUE = "1";
    public static final String COMM_FALSE = "0";

    //公共编号长度
    public static final int COMM_NUM_BASE = 10;
    public static final int COMM_NUM_LENGTH = 6;
    public static final int COMM_NUM_RANGESIZE = 2;
    public static final String KEY_COMM_NUM_MAX = "KEY_COMM_NUM_MAX";



    public static final String KEY_NUMS = "KEY_NUMS";
    public static final String KEY_TOTAL_COUNT = "KEY_TOTAL_COUNT";

    //默认有效期
    public static final int DEF_SHELF_LIFE = 18;
    public static final int CANCEL_NUM = 4;

    //订单支付KEY
    public static final String KEY_ORDER = "KEY_ORDER";
    public static final String KEY_ORDER_PAY = "KEY_ORDER_PAY";
    public static final String KEY_ORDER_DETAIL = "KEY_ORDER_DETAIL";
    public static final String HDFS_ORDER = "order";
    public static final String HDFS_ORDER_PAY = "order_pay";
    public static final String HDFS_ORDER_DETAIL = "order_detail";
    public static final String KEY_ORDER_STATUS = "KEY_ORDER_STATUS";

    //用户KEY
    public static final String KEY_CUSTOMER = "KEY_CUSTOMER";
    public static final String KEY_CUSTOMER_ATT = "KEY_CUSTOMER_ATT";
    public static final String KEY_RELEASE = "KEY_RELEASE";
    public static final String KEY_PAGES = "KEY_PAGES";
    public static final String KEY_PAGES_PRODUCT = "KEY_PAGES_PRODUCT";
    public static final String KEY_ACTION_LOG = "KEY_ACTION_LOG";
    public static final String KEY_ACTION_LOG_COMMENT = "KEY_ACTION_LOG_COMMENT";

    public static final int ACTION_LOG_DAY_RANGE = 7;

    public static final String KEY_EXTPARAMS_COUNT = "KEY_EXTPARAMS_COUNT";
    public static final int KEY_EXTPARAMS_COUNT_VALUE = 10;
    public static final int KEY_EXTPARAMS_COUNT_VALUE_DEF = 10;

    //时间key
    public static final String KEY_TIMESTAMP = "KEY_TIMESTAMP";
    public static final String KEY_TM_TIMESTAMP = "KEY_TM_TIMESTAMP";
    public static final String KEY_DAY = "KEY_DAY";
    public static final String KEY_NCT = "KEY_NCT";
    public static final String KEY_COMM_SUCC = "KEY_COMM_SUCC";

    //用户日志
    public static final String KEY_USERLOG_ACTION = "KEY_USERLOG_ACTION";
    public static final String KEY_USER_ACTIONLOG_LAUNCH = "KEY_USER_ACTIONLOG_LAUNCH";
    public static final String KEY_USER_ACTIONLOG_EXIT = "KEY_USER_ACTIONLOG_EXIT";
    public static final String KEY_USER_ACTIONLOG_MAXCT = "KEY_USER_ACTIONLOG_MAXCT";
    public static final String KEY_USER_ACTIONLOG_IS_LAUEXIT = "1";

    /**
     * 店铺页面
     * 01 店铺首页
     * 02 店铺商品
     * 03 店铺活动
     * 04 店铺上新
     * 05 店铺动态
     */
    public static final String PAGE_SUPPLIER_COUNT = "KEY_EXTPARAMS";




    //商品分类KEY
    public static final String KEY_EXTPARAMS = "KEY_EXTPARAMS";
    public static final String KEY_CATEGORYS = "KEY_CATEGORYS";
    public static final String KEY_CATEGORY = "KEY_CATEGORY";
    public static final String KEY_EXTPARAMS_CATEGORY_COUNT = "KEY_EXTPARAMS_CATEGORY_COUNT";
    public static final int KEY_EXTPARAMS_CATEGORY_COUNT_VALUE = 10;
    public static final int KEY_EXTPARAMS_CATEGORY_COUNT_VALUE_DEF = 1;


    public static final String KEY_CATEGORY_SELECT = "KEY_CATEGORY_SELECT";
    public static final String KEY_SUPPLIERS_PAGES = "KEY_SUPPLIERS_PAGES";
    public static final String KEY_SUPPLIERS = "KEY_SUPPLIERS";
    public static final String KEY_PRODUCT_PAGES = "KEY_PRODUCT_PAGES";
    public static final String KEY_PRODUCT_PAGE = "KEY_PRODUCT_PAGE";
    public static final String KEY_PRODUCTS = "KEY_PRODUCTS";
    public static final String KEY_PRODUCTS_MAP = "KEY_PRODUCTS_MAP";

    //分类、店铺、商品描述
    public static final String REMARK_CATEGORY_FCLS = "FC";
    public static final String REMARK_CATEGORY_SCLS = "SC";
    public static final int CATEGORY_FCLS_LEVEL = 1;
    public static final int CATEGORY_SCLS_LEVEL = 2;

    public static final String REMARK_SHOPER = "S";
    public static final String REMARK_GOODS = "G";

    //价格
    public static final int PRICE_INTLEN = 3;
    public static final int PRICE_LEVEL_ONE = 1;
    public static final int PRICE_LEVEL_TWO = 2;
    public static final int PRICE_LEVEL_THREE = 3;
    public static final int PRICE_LEVEL_FOUR = 4;
    public static final int PRICE_LEVEL_MAX = 5;
    public static final int PRICE_DECILEN = 1;

    public static final int PRICE_MARKET_DIFF = 100;





    //页面
    public static final int PAGE_PAGES = 4;
    public static final int PAGE_TOPS = 5;
    public static final int PAGE_FCLS = 5;
    public static final int PAGE_SCLS = 8;
    public static final int PAGE_SHOPERS = 20;
    public static final int PAGE_SHOPERS_CHILD = 4;
    public static final int PAGE_GOODS = 5;
    public static final int PAGE_GOODS_CHILD = 4;

    public static final String MAIN_PAGE_CODE = "100";
    public static final String CART_PAGE_CODE = "101";
    public static final String LOGIN_PAGE_CODE = "102";
    public static final String REGISTER_PAGE_CODE = "103";


    //页面标签页
    public static final Integer SUPPLIER_TABPAGE_COUNT_DEF = 5;
    public static final Integer PRODUCT_TABPAGE_COUNT_DEF = 4;



    //通用编码位数
    public static final int COMMON_NUM_RANGESIZE = 2;

    //参数
    public static final String ACTIONLOG_USER_NAME = "user_name";
    public static final String ACTIONLOG_USER_PASS = "user_pass";
    public static final String ACTIONLOG_VALIDATE_TYPE = "validate_type";
    public static final String ACTIONLOG_EVENT_RESULT = "event_result";


    public static final int ACTIONLOG_EVENT_VIEW_PRODUCTS_COUNT = 4;


    public static final int RESULT_SUC = 1;
    public static final int RESULT_FAILURE = 0;

    public static final int VALIDATE_TYPE_PASS = 1;//密码
    public static final int VALIDATE_TYPE_CODE = 2;//验证码


    public static final String ACTIONLOG_TARGET_ACTION = "target_action";
    public static final String ACTIONLOG_TARGET_ID = "target_id";
    public static final String ACTIONLOG_TARGET_IDS = "target_ids";
    public static final String ACTIONLOG_TARGET_TYPE = "target_type";
    public static final String ACTIONLOG_CONTENT = "target_content";
    public static final String ACTIONLOG_ORDER = "target_order";
    public static final String ACTIONLOG_KEYS = "target_keys";

    public static final String ACTIONLOG_CONTENT_PRICE = "price";
    public static final String ACTIONLOG_CONTENT_IMG = "image";
    public static final String ACTIONLOG_CONTENT_COUNT = "count";

    public static final String ACTIONLOG_TARGET_PAY = "target_pay";
    public static final String ACTIONLOG_TARGET_PAY_TYPE = "pay_type";
    public static final String ACTIONLOG_TARGET_PAY_CODE = "pay_code";

    //商品分类数量
    public static final int CATEGORY_NUM_FIRST = 10;
    public static final int CATEGORY_NUM_SECOND = 10;


    //店铺数量
    public static final int SUPPLIER_NUM = 100;

    public static final int CATEGORY_NUM_RANGESIZE = 2;

    //商品编号范围
    public static final int PRODUCT_NUM = 100;
    public static final int PRODUCT_NUM_MIN = 1;
    public static final int PRODUCT_NUM_MAX = 100;
    public static final int PRODUCT_CODE_LEN = 2;
    public static final int PRODUCT_CODE_LEVEL = 2;
    public static final int PRODUCT_NUM_RANGESIZE = 2;
    public static final int PRODUCT_VIEW_SIZE = 8;

    //public static final int ORDER_END = 4;

    public static final String PREFIX_ZERO = "0";



    //用户数量
    public static final int CUSTOMER_COUNT = 4;

    //用户登录方式
    public static final int CUSTOMER_LOGIN_MOBILE = 1;
    public static final int CUSTOMER_LOGIN_NAME = 2;




}
