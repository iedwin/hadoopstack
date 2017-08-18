package com.xiaoxiaomo.proxy.service;

import com.xiaoxiaomo.proxy.ResultMsg;
import com.xiaoxiaomo.proxy.dao.DemoDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;

/**
 *
 * Created by xiaoxiaomo on 2017/2/21.
 */
@Service
public class DemoService {
    private static final Logger LOG = LoggerFactory.getLogger(DemoService.class);

    @Resource
    DemoDao demoDao;

    /**
     *
     * @param text
     * @param type
     * @return
     */
    public Map<String, Object> demo(
            final String text ,final String type ) throws Exception {
        LOG.info(" ......DemoService....... ");
        return ResultMsg.success(demoDao.getDemo(text)) ;
    }

}
