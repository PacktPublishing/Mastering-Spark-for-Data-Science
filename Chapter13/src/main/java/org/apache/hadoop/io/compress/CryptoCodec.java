package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.crypto.CryptoCompressor;
import org.apache.hadoop.io.compress.crypto.CryptoDecompressor;

public class CryptoCodec implements CompressionCodec, Configurable {

    private static final Log LOG = LogFactory.getLog(CryptoCodec.class);

    public static final String CRYPTO_DEFAULT_EXT = ".crypto";

    private Configuration config;

    @Override
    public Compressor createCompressor() {
        LOG.info("Creating Compressor");
        try {
            return new CryptoCompressor();
        }
        catch(Exception e){
            LOG.error(e.getMessage());
            return null;
        }
    }

    @Override
    public Decompressor createDecompressor() {
        LOG.info("Creating Decompressor");
        try {
            return new CryptoDecompressor();
        }
        catch(Exception e){
            LOG.error(e.getMessage());
            return null;
        }
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in) throws IOException {
        return createInputStream(in, createDecompressor());
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decomp) throws IOException {
        LOG.info("Creating DecompressorStream stream");
        return new DecompressorStream(in, decomp);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return createOutputStream(out, createCompressor());
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor comp) throws IOException {
        LOG.info("Creating CompressorStream stream");
        return new CompressorStream(out, comp);
    }

    @Override
    public Class<? extends Compressor> getCompressorType() {
        return CryptoCompressor.class;
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        return CryptoDecompressor.class;
    }

    @Override
    public String getDefaultExtension() {
        return CRYPTO_DEFAULT_EXT;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
    }

}
