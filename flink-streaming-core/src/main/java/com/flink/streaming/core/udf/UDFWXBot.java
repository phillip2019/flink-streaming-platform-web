package com.flink.streaming.core.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flink.streaming.core.util.JacksonBuilder;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * @description 发送消息给企业微信机器人
 * @author xiaowei.song
 * @date 2021-09-28 21:41
 * @version v1.0.0       
 **/
public class UDFWXBot extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(UDFWXBot.class);

    public static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    public static final String ERROR_WX_MSG = "{\\\"msgtype\\\":\\\"markdown\\\",\\\"markdown\\\":{\\\"content\\\":\\\"消息序列化失败，请检查异常!!!\\\"}}";
    private VelocityEngine ve = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        ve = new VelocityEngine();
        ve.init();
    }

    /**
     * @description 发送企业微信机器人
     * @return 是否发送成功
     **/
    public Boolean eval(String wxWebHook, String markdownTemplate, Map<String, String> params) throws IOException {
        if (StringUtils.isBlank(wxWebHook)) {
            logger.error("微信机器人地址输入为空，请检查之后再试");
            return Boolean.FALSE;
        }

        if (!StringUtils.startsWith(wxWebHook, "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?")) {
            logger.error("微信机器人地址输入错误，请检查之后再试, 微信机器人地址必须https://qyapi.weixin.qq.com/cgi-bin/webhook/send?开头， 输入地址为: {}", wxWebHook);
            return Boolean.FALSE;
        }

        if (StringUtils.isBlank(markdownTemplate)) {
            logger.error("微信发送消息模板为空，请检查之后再试");
            return Boolean.FALSE;
        }

        VelocityContext vc = new VelocityContext(params);
        StringWriter wxMsgWriter = new StringWriter();
        ve.evaluate(vc, wxMsgWriter, "wxMessage",  markdownTemplate);

        OkHttpClient client = new OkHttpClient();

        ObjectNode resOn = JacksonBuilder.mapper.createObjectNode();
        resOn.put("msgtype", "markdown");
        ObjectNode resFileOn = JacksonBuilder.mapper.createObjectNode();
        resOn.set("markdown", resFileOn.put("content", wxMsgWriter.toString()));

        RequestBody body = null;
        try {
            body = RequestBody.create(JSON_MEDIA_TYPE, JacksonBuilder.mapper.writeValueAsString(resOn));
        } catch (JsonProcessingException e) {
            logger.error("序列化参数值异常， 参数值为: {}", resOn, e);
            body = RequestBody.create(ERROR_WX_MSG, JSON_MEDIA_TYPE);
        }
        Request webHookRequest = new Request.Builder()
                .url(wxWebHook)
                .post(body)
                .build();

        Response webHookResponse = null;
        try {
            webHookResponse = client.newCall(webHookRequest).execute();
        } catch (IOException e) {
            logger.error("发送消息失败，异常为: ", e);
        }

        if (webHookResponse == null || !webHookResponse.isSuccessful()) {
            return Boolean.FALSE;
        }

        JsonNode rspWkJn = JacksonBuilder.mapper.readTree(webHookResponse.body().string());
        int errorCode = rspWkJn.path("errcode").asInt(-1);
        if (errorCode != 0) {
            logger.error("发送失败，wxWebHook errorCode: {}, errorInfo: {}", errorCode, rspWkJn.path("errmsg").asText(""));
            return Boolean.FALSE;

        }

        logger.info("发送成功， {}", rspWkJn.toString());
        return Boolean.TRUE;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public static void main(String[] args) {
        Map<String, String> m = new HashMap<>();
        m.put("k1", "v1");
        m.put("k2", "v2");
        VelocityContext vc = new VelocityContext(m);
        StringWriter wxMsgWriter = new StringWriter();
        VelocityEngine ve = new VelocityEngine();
        ve.init();
        ve.evaluate(vc, wxMsgWriter, "wxMessage",  "测试发送消息");
    }
}