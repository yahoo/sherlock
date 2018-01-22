package com.yahoo.sherlock.store.redis;

import com.yahoo.sherlock.store.Attribute;
import com.yahoo.sherlock.utils.NumberUtils;

import lombok.extern.slf4j.Slf4j;
import spark.utils.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The byte mapper class writes objects and their attributes
 * to compressed bytes. These are not human readable.
 */
@Slf4j
public class ByteMapper extends Mapper<byte[]> {

    /**
     * Compress a string using GZIP to bytes to be written
     * to Redis. The string is compressed as UTF-8.
     *
     * @param str string to compress
     * @return compressed byte array
     * @throws IOException if an error occurs during compression
     */
    protected byte[] compressUTF8(String str) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(str.length())) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
                gzip.write(str.getBytes(StandardCharsets.UTF_8));
                gzip.close();
            }
            return bos.toByteArray();
        }
    }

    /**
     * Decompress a string encoded as UTF-8 bytes to a
     * Java String.
     *
     * @param compressed string as compressed bytes
     * @return decompressed String
     * @throws IOException if an error occurs during decompression
     */
    protected String decompressUTF8(byte[] compressed) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed)) {
            try (GZIPInputStream gzip = new GZIPInputStream(bis)) {
                byte[] decompressed = IOUtils.toByteArray(gzip);
                return new String(decompressed, StandardCharsets.UTF_8);
            }
        }
    }

    /**
     * Compress an integer by storing in as few bytes
     * as possible to maintain the value.
     *
     * @param n the integer to compress
     * @return a byte array representation
     */
    protected byte[] compressInt(int n) {
        return NumberUtils.toBytesCompressed(n);
    }

    /**
     * Decompress an integer represented as a byte array.
     *
     * @param compressed byte array
     * @return the represented integer
     */
    protected int decompressInt(byte[] compressed) {
        return NumberUtils.decodeBytes(compressed);
    }

    /**
     * Shorthand method to delegate an attribute type to the
     * correct compression method.
     *
     * @param data the data to compress
     * @param type the attribute type
     * @return data compressed as bytes
     * @throws IOException if an error occurs during compression
     */
    protected byte[] compress(Object data, Attribute.Type type) throws IOException {
        return type == Attribute.Type.INTEGER ? compressInt(Integer.parseInt(data.toString())) : compressUTF8(data.toString());
    }

    /**
     * Shorthand method to delegate an attribute type to the
     * correct decompression method.
     *
     * @param compressed the byte array to decompress
     * @param type       the attribute type
     * @return the decompressed value as a string
     * @throws IOException if an error occurs during decompression
     */
    protected String decompress(byte[] compressed, Attribute.Type type) throws IOException {
        return type == Attribute.Type.INTEGER ? String.valueOf(decompressInt(compressed)) : decompressUTF8(compressed);
    }

    @Override
    protected Map<byte[], byte[]> performMap(Object obj) {
        Map<byte[], byte[]> map = new HashMap<>((int) (1.8 * getFields().length));
        for (Field field : getFields()) {
            try {
                String name = field.getName();
                Object data = field.get(obj);
                Attribute.Type attrType = resolveType(field);
                byte[] compressed = compress(data, attrType);
                map.put(compressUTF8(name), compressed);
            } catch (IllegalAccessException e) {
                log.error("Cannot access field: {}", field.getName(), e);
            } catch (IOException e) {
                log.error("Error while compressing UTF-8!", e);
            }
        }
        return map;
    }

    @Override
    protected <C> C performUnmap(Class<C> cls, Map<byte[], byte[]> map) {
        C obj = construct(cls);
        for (Field field : getFields()) {
            String name = field.getName();
            try {
                byte[] key = compressUTF8(name);
                byte[] compressed = map.get(key);
                if (compressed == null) {
                    continue;
                }
                Attribute.Type attrType = resolveType(field);
                String value = decompress(compressed, attrType);
                field.set(obj, value);
            } catch (IOException e) {
                log.error("Error while decompressing UTF-8!", e);
            } catch (IllegalAccessException e) {
                log.error("Cannot access field: {}", field.getName(), e);
            }
        }
        return obj;
    }
}
