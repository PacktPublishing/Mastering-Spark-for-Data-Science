package org.apache.hadoop.io.compress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.spec.InvalidParameterSpecException;
import java.util.Arrays;

public class EncryptionUtils {

    private static final Log log = LogFactory.getLog(EncryptionUtils.class);

    private static final String HEXES = "0123456789ABCDEF";
    private final String encoding = "AES/CBC/PKCS5Padding";

    private Cipher ecipher;
    private Cipher dcipher;
    private SecretKeySpec skeySpec = null;

    public EncryptionUtils() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException, UnsupportedEncodingException {
        this.setupCrypto(getPassword());
    }

    private void setupCrypto(String password) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, UnsupportedEncodingException {
        IvParameterSpec paramSpec = new IvParameterSpec(generateIV());
        skeySpec = new SecretKeySpec(password.getBytes("UTF-8"), "AES");
        ecipher = Cipher.getInstance(encoding);
        ecipher.init(Cipher.ENCRYPT_MODE, skeySpec, paramSpec);
        dcipher = Cipher.getInstance(encoding);
    }

    private String getPassword(){
        // Use a Java KeyStore as per the below code, a Database or any other secure mechanism to obtain a password
        // TODO We will return a hard coded String for simplicity
        return "keystorepassword";
    }


    private byte[] generateIV() {
        SecureRandom random = new SecureRandom();
        byte bytes[] = new byte[16];
        random.nextBytes(bytes);
        return bytes;
    }

    public byte[] encrypt(byte[] plainBytes, boolean addIV) throws UnsupportedEncodingException, InvalidParameterSpecException {
        byte[] iv = "".getBytes("UTF-8");
        if (!addIV) {
            iv = ecipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();
        }
        byte[] ciphertext = ecipher.update(plainBytes, 0, plainBytes.length);
        byte[] result = new byte[iv.length + ciphertext.length];
        System.arraycopy(iv, 0, result, 0, iv.length);
        System.arraycopy(ciphertext, 0, result, iv.length, ciphertext.length);
        return result;

    }

    public byte[] doFinal() throws BadPaddingException, IllegalBlockSizeException {
        byte[] ciphertext = ecipher.doFinal();
        return ciphertext;
    }

    public byte[] doFinalDecrypt() throws BadPaddingException, IllegalBlockSizeException {
        byte[] ciphertext = dcipher.doFinal();
        return ciphertext;
    }

    public byte[] decrypt(byte[] ciphertext, boolean useIV) throws InvalidAlgorithmParameterException, InvalidKeyException {
        byte[] deciphered;
        if (useIV) {
            byte[] iv = Arrays.copyOfRange(ciphertext, 0, 16);
            IvParameterSpec paramSpec = new IvParameterSpec(iv);
            dcipher.init(Cipher.DECRYPT_MODE, skeySpec, paramSpec);
            deciphered = dcipher.update(ciphertext, 16, ciphertext.length - 16);
        } else {
            deciphered = dcipher.update(ciphertext, 0, ciphertext.length);
        }
        return deciphered;
    }

    private static byte[] getMD5(String input) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] bytesOfMessage = input.getBytes("UTF-8");
        MessageDigest md = MessageDigest.getInstance("MD5");
        return md.digest(bytesOfMessage);
    }

    public static String byteToHex(byte[] raw) {
        if (raw == null) {
            return null;
        }
        final StringBuilder hex = new StringBuilder(2 * raw.length);
        for (final byte b : raw) {
            hex.append(HEXES.charAt((b & 0xF0) >> 4)).append(HEXES.charAt((b & 0x0F)));
        }
        return hex.toString();
    }

    public static byte[] hexToByte(String hexString) {
        int len = hexString.length();
        byte[] b = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            b[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4) + Character.digit(hexString.charAt(i + 1),
                    16));
        }
        return b;
    }

    public static SecretKey createSecretKey() {
        KeyGenerator kg = null;
        try {
            kg = KeyGenerator.getInstance("AES");
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        kg.init(128);
        SecretKey sk = kg.generateKey();

        return sk;
    }

    // Use KeyTool on the command line to work with a Java KeyStore
    // keytool -genseckey -alias secretkeyalias -keyalg AES -keystore testkeystore.jceks -keysize 128 -storeType JCEKS

    public static void storeKey(SecretKey sk, String password, String keyStorePath, String secretKeyAlias) throws Exception {
        KeyStore ks = KeyStore.getInstance("JCEKS");

        char[] passwd = password.toCharArray();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(keyStorePath);
            ks.load(fis, passwd);
        } finally {
            if (fis != null) {
                fis.close();
            }

            KeyStore.ProtectionParameter protParam =
                    new KeyStore.PasswordProtection(passwd);

            KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(sk);
            ks.setEntry(secretKeyAlias, skEntry, protParam);
        }
    }

    // keytool -v -list -storetype JCEKS -keystore testkeystore.jceks

    public static SecretKey retrieveKey(String password, String keyStorePath, String secretKeyAlias) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance("JCEKS");
        keyStore.load(new FileInputStream(keyStorePath), password.toCharArray());
        SecretKey key = (SecretKey) keyStore.getKey(secretKeyAlias, password.toCharArray());
        return key;
    }

    public static void createJceksStore(String password, String keyStorePath) throws Exception {
        KeyStore ks = KeyStore.getInstance("JCEKS");

        char[] passwd = password.toCharArray();
        ks.load(null, null);

        FileOutputStream fos = new FileOutputStream(keyStorePath);
        ks.store(fos, passwd);
        fos.close();
    }
}