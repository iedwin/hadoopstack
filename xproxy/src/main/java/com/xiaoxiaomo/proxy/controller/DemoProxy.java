package com.xiaoxiaomo.proxy.controller;

import com.xiaoxiaomo.proxy.ResultMsg;
import com.xiaoxiaomo.proxy.service.DemoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 基于SpringMVC实现的代理服务
 */
@Controller
@RequestMapping("/proxy")
public class DemoProxy {
    private static final Logger LOG = LoggerFactory.getLogger(DemoProxy.class);
    @Resource
    DemoService demoService;

    @RequestMapping("/demo")
    @ResponseBody
    public Map<String, Object> demo(
            @RequestParam(value = "text", required = true) String text,
            @RequestParam(value = "type", required = false) String type) {

        try {
            return demoService.demo(text, type);
        } catch (Exception e) {
            LOG.error("获取黑名单信息，系统异常！",e);
            return ResultMsg.fail(e.getMessage());
        }
    }

}