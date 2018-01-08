package com.xiaoxiaomo.common.rsa;


import com.sun.jersey.core.util.Base64;

/**
 * Created by TangXD on 2017/12/19.
 */
public class RSATest {
//    static String publicKey;
//    static String privateKey;
//
//    static {
//        try {
//            Map<String, Object> keyMap = RSAUtils.genKeyPair();
//            publicKey = RSAUtils.getPublicKey(keyMap);
//            privateKey = RSAUtils.getPrivateKey(keyMap);
//            System.err.println("公钥: \n\r" + publicKey);
//            System.err.println("私钥： \n\r" + privateKey);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }



    public static void main(String[] args) throws Exception {

        String privateKey="MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAMF4p4qiAQdpwnWssa7hzldsq56L/QuXc4ViBu7z/HabdU1eO+Z0VmMlsWAk7rBSiTyTMOp+s6cvDmJ9M2KffA/ceLcsKoCre6Tz5+Fvr2fPouXjoruOZVj57LIiJEzYtlgjXczUq40cz8dSBZON5SEMxE3CIBjhuv17CWK5RbWHAgMBAAECgYBy3Cq7h0qNGCC2s/wZEz3pcT7CNeHZC+UtfvEW1AtCbzDI6fUt8EuJOUuBBOwgCiAnsksMLISD8M/dcO8c0gpYZ/+JlneZGW1N93AsE5j3cUMvgiK7OZojw3rmp16CgPDy2rqRbbGr+uKK1ruYyC4H0mDqzyPd9JViP1XX2ruDAQJBAONvEkBUbZiXigP7u8u1RoDjy6aRL8mZD308j/KE8R3yNqMY0950bJa3wqYq1gS2lYd2RPTGRVusPj0MtSURpGcCQQDZxYw/Y7/0B6pePEpARpd2hdQQk1b/erDUQikr+55fIPUhVEnxKwks9nomD7+/gtEaqEyLqytl7VSKz2Olf7HhAkAYflPG3dDXyCoy959n9uPa6a17CNPOsE88u5L9GVgmU3mS4w+eO4eeS7gI0UAvTcKYziHrApdhohEp3f58OYQtAkB+6t4Q956gp3MIVtTjXwDZJ6fvPR+/5451NlXud0fWo0uZ8BjkADPpy4Bm/FjBUYlyotFxuQGi9s2F1NZ7Vs+hAkALjM700xqWzZIDxAnKpHeOIrTAIyLVSvW1nl" +
                "TJosqO6VECLTzb1tMbbM14MlrplkABB7E9U+V68apx3O4KQsWP";
        byte[] encodedData= Base64.decode("VtmI971kLQw/BipjMVtiqymzUN02y/xX9eA0VkgWVzJ6lni6vVhRhGAX+Q+ZTcjAShHqznvBEIng7TwY3bJ+dQFSD2vXEiNB8wWKFgU34bSsfoJiukXgyOIL0QsFy0zsfwsN185ayAFWS3ft+ippCXjCcsE+TNGm06uwnXoDq00=");

//        byte[] decodedData = RSAUtils.decryptByPrivateKey(encodedData.getBytes(), privateKey);
//        String target = new String(decodedData);
//        System.out.println("解密后文字: \r\n" + target);


//        System.err.println("公钥加密——私钥解密");
//        String source = "这是一行没有任何意义的文字，你看完了等于没看，不是吗？";
//        System.out.println("\r加密前文字：\r\n" + source);
//        byte[] data = source.getBytes();
//        byte[] encodedData = RSAUtils.encryptByPublicKey(data, publicKey);
//        System.out.println("加密后文字：\r\n" + new String(encodedData));

        byte[] decodedData = RSAUtils.decryptByPrivateKey(encodedData, privateKey);
        String target = new String(decodedData);
        System.out.println("解密后文字: \r\n" + target);
    }

}
