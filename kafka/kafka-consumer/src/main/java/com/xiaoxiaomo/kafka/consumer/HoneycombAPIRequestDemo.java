package com.creditease.test;

import javax.crypto.Cipher;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.security.Key;
import java.security.KeyFactory;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 该类用于构造请求参数，并提交请求给蜂巢网关的示例类。
 * 该类可以直接运行
 *
 * @version 1.00 Mar 9, 2017
 * @author <a href="mailto:wushexin@gmail.com">Frank Wu</a>
 */
public class HoneycombAPIRequestDemo {
    /** 加密算法RSA */
    public static final String KEY_ALGORITHM = "RSA";

    /** 签名算法 */
    public static final String SIGNATURE_ALGORITHM = "MD5withRSA";

    /**RSA最大加密明文大小*/
    private static final int MAX_ENCRYPT_BLOCK = 117;

    /**
     * <p>
     * 公钥加密
     * </p>
     *
     * @param data
     *            源数据
     * @param publicKey
     *            公钥(BASE64编码)
     * @return
     * @throws Exception
     */
    public static byte[] encryptByPublicKey(byte[] data, String publicKey)
            throws Exception {
        byte[] keyBytes =  Base64.getDecoder().decode(publicKey);//Base64Utils.decode(publicKey);
        X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        Key publicK = keyFactory.generatePublic(x509KeySpec);
        // 对数据加密
        Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
        cipher.init(Cipher.ENCRYPT_MODE, publicK);
        int inputLen = data.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;
        byte[] cache;
        int i = 0;
        // 对数据分段加密
        while (inputLen - offSet > 0) {
            if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
                cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
            } else {
                cache = cipher.doFinal(data, offSet, inputLen - offSet);
            }
            out.write(cache, 0, cache.length);
            i++;
            offSet = i * MAX_ENCRYPT_BLOCK;
        }
        byte[] encryptedData = out.toByteArray();
        out.close();
        return encryptedData;
    }


    /**
     * 向指定URL发送GET方法的请求
     *
     * @param url   发送请求的URL
     * @param param 请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return URL 所代表远程资源的响应结果
     */
    public static String sendGet(String url, String param) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 遍历所有的响应头字段
            for (String key : map.keySet()) {
                System.out.println(key + "--->" + map.get(key));
            }
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    /*
     * 该方法中有请求参数
     * @param args
     * @throws Exception
     * void
     * @exception
     */
    public static void main(String[] args) throws Exception {
        //准备请求参数 cleintId 和 sourceCode
        //蜂巢网关测试访问地址
        String url = "http://honeycomb-api.yixinonline.com";

        //测试用clientId
        String clientId = "INC-TECH-PRODUCT";

        //测试 sourceCode
        String sourceCode = "abcApp";

        //测试用RSA public key
        String key = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCP3zGvjew0krj08v+ase3pI359zAJ5Q4xF7Ilx4XFgLlr1wZmvKAs0j2DwF0Ls" +
                "WQKWJcs5V9u85hESGeKOo9Fo0LP/OyYo2RFGm9yOz/ioTPM8sNGSIQZjxI+IWCa9xbysXSNd7eZvkIiG89i9cla7wa1ibrlBnsvX5iNWyqMAyQIDAQAB";

        //原始queryterms参数值，必须符合JSON格式，可以尝试调用 com.alibaba.fastjson.JSONObject.parse(params); 验证之
//        String params = "{\"op\":\"/if/getLoginImageCode\",\"sourceCode\":\""+sourceCode+"\",\"location_cid\":\"1100\"}";
        String params = "";

        //RSA加密后，Base64编码，注意UTF-8格式
        String queryterms = Base64.getEncoder().encodeToString(encryptByPublicKey(params.getBytes("UTF-8"), key));

        //URLEncoder 注意UTF-8格式
        queryterms = URLEncoder.encode(queryterms,"UTF-8");

        //构建请求参数
        String param = "service=gjj&clientId="+clientId + "&queryterms="+ queryterms;

        //发送请求并得到结果
        String response = sendGet(url,param);
        //如打印包含类似的结果 代表请求成功 {"errors":"","status":"1",”….”}
        System.out.println(response);
    }
}
