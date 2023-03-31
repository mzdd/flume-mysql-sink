package com.mzdd.flume.business.entity;

import java.io.Serializable;

/**
 * 样例数据结构
 *
 * @author mzdd
 * @create 2023-03-31 13:36
 */
public class DemoData implements Serializable {

    /**
     * <pre>
     * 主键
     * </pre>
     */
    private String	traceId;

    /**
     * <pre>
     * 产品id
     * </pre>
     */
    private String	productId;

    /**
     * <pre>
     * 产品名称
     * </pre>
     */
    private String	productName;

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }
}
