package com.xiaoxiaomo.proxy.dao;

import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 *
 * demo dao
 */
@Service
public class DemoDao {

    /**
     * demo
     * @param text
     * @return
     * @throws IOException
     */
    public String getDemo(String text) throws IOException {
        return text ;
    }

}
