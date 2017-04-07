package org.apache.hadoop.io.compress.crypto;


import org.apache.hadoop.io.compress.EncryptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;


public class CryptoDecompressor implements Decompressor {

    private static final Log LOG = LogFactory.getLog(CryptoDecompressor.class);

    private EncryptionUtils crypto;
    private byte[] in;
    private ByteBuffer out;
    private boolean finished = false;
    private boolean useIV = true;

    public CryptoDecompressor() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException, UnsupportedEncodingException {
        crypto = new EncryptionUtils();
    }

    @Override
    public synchronized int decompress(byte[] buf, int off, int len) throws IOException {
        if (needsInput()) {
            return 0;
        }

        ensureBuffer(len);

        byte[] decrypt = new byte[0];
        try {
            decrypt = crypto.decrypt(in, useIV);
        } catch (InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        if (useIV) {
            useIV = false;
        }
        byte[] result = null;

        if (finished) {
            byte[] doFinal = new byte[0];
            try {
                doFinal = crypto.doFinalDecrypt();
            } catch (BadPaddingException e) {
                e.printStackTrace();
            } catch (IllegalBlockSizeException e) {
                e.printStackTrace();
            }
            byte[] join = new byte[decrypt.length + doFinal.length];
            System.arraycopy(decrypt, 0, join, 0, decrypt.length);
            System.arraycopy(doFinal, 0, join, decrypt.length, doFinal.length);
            result = join;
        } else {
            result = decrypt;
        }

        in = null;
        if (result == null) {
            throw new IOException("Error: Decryption result is null");
        }
        ensureBuffer(out.position() + result.length);
        out.put(result);
        return flushBuffer(buf, off, len);
    }

    private void ensureBuffer(int n) {
        if (out == null) {
            out = ByteBuffer.allocate(n * 2);
        } else if (out.capacity() < n) { // Grow
            ByteBuffer newBuffer = ByteBuffer.allocate(n);
            out.flip();
            newBuffer.put(out);
            out = newBuffer;
        }
    }

    @Override
    public int getRemaining() {
        return 0;
    }

    private int flushBuffer(byte[] buf, int off, int len) {
        int size = Math.min(Math.min(len, buf.length - off), out.position());
        if (size <= 0)
            return 0;
        out.flip();
        out.get(buf, off, size);
        out.compact();
        return size;
    }

    @Override
    public void end() {
    }

    @Override
    public boolean finished() {
        return finished;
    }

    @Override
    public boolean needsInput() {
        return (in == null || in.length < 0);
    }

    @Override
    public void reset() {
        in = null;
        finished = false;
    }

    @Override
    public synchronized void setInput(byte[] buf, int offset, int length) {
        if (length > 0) {
            in = new byte[length];
            int inIdx = 0;
            for (int i = offset; i < length; i++) {
                in[inIdx] = buf[i];
                inIdx++;
            }
        }
        if (offset + length < buf.length) {
            finished = true;
        }
    }

    @Override
    public boolean needsDictionary() {
        return false;
    }

    @Override
    public void setDictionary(byte[] arg0, int arg1, int arg2) {
    }
}

