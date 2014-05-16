package org.renyan.kv.impl;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.renyan.kv.SDB;
import org.renyan.kv.util.MD5Util;
import org.renyan.kv.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple Key/Value DB 实现
 * User: weirenan
 * Date: 14-5-16
 * Time: 下午9:20
 */
public class SDBImpl implements SDB {
    private Logger logger = LoggerFactory.getLogger(SDBImpl.class);

    private File dataPath; //系统数据目录

    private final ReentrantLock mutex = new ReentrantLock();

    private String DATA_NAME = "data";
    private String DATA_LOCK = "LOCK";
    private DBLock dbLock;
    private String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public SDBImpl(String database) throws IOException {

        Preconditions.checkNotNull(database, "lockFile is null");

        mutex.lock();

        try {

            dataPath = new File(database);

            dataPath.mkdirs();

            dbLock = new DBLock(new File(dataPath, DATA_LOCK));

        } finally {
            mutex.unlock();
        }
    }

    @Override
    public <T> boolean put(String key, String group, T body) {

        Preconditions.checkArgument(StringUtils.isBlank(key), "key can't is null or empty");
        Preconditions.checkArgument(StringUtils.isBlank(group), "group can't is null or empty");
        Preconditions.checkNotNull(body, "value can't is null or empty");

        mutex.lock();
        try {
            Gson gson = new GsonBuilder().setDateFormat(DATE_FORMAT).create();
            String json;
            if (body instanceof String) {
                json = (String) body;
            } else {
                json = gson.toJson(body);
            }
            boolean b = writeData(key, group, json);
            return b;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public <T> T get(String key, String group, Class<T> clazz) {

        Preconditions.checkArgument(StringUtils.isBlank(key), "key can't is null or empty");
        Preconditions.checkArgument(StringUtils.isBlank(group), "group can't is null or empty");

        mutex.lock();
        try {
            File keyFile = getKeyFile(key, group);
            List<T> list = readData(keyFile, clazz);
            if (list == null || list.isEmpty()) {
                return null;
            }
            return list.get(0);
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public <T> List<T> list(String group, Class<T> clazz) {

        if (StringUtils.isBlank(group)) {
            return null;
        }

        if (clazz == null) {
            return null;
        }

        File dataFile = getGroupCurrentFile(group);
        mutex.lock();
        try {
            List<T> ts = readData(dataFile, clazz);
            return ts;
        } finally {
            mutex.unlock();
        }
    }


    @Override
    public boolean delete(String key, String group) {
        Preconditions.checkArgument(StringUtils.isBlank(key), "key can't is null or empty");
        Preconditions.checkArgument(StringUtils.isBlank(group), "group can't is null or empty");

        File keyFile = getKeyFile(key, group);
        mutex.lock();
        try {
            return keyFile.delete();
        } finally {
            mutex.unlock();
        }
    }

    /**
     * 删除key 存储数据文件
     *
     * @param keyFile
     * @return
     */
    private boolean deleteData(File keyFile) {
        mutex.lock();
        try {
            return keyFile.delete();
        } finally {
            mutex.unlock();
        }
    }

    /**
     * 写入Key 数据内容到数据文件
     *
     * @param key   key
     * @param group 分组
     * @param body  内容
     * @return
     */
    private boolean writeData(String key, String group, String body) {

        // get key file
        File file = getKeyFile(key, group);

        //init create  key file parent dir
        File parentFile = file.getParentFile();

        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }

        RandomAccessFile fileWriter = null;
        boolean execStatus = false;
        try {
            fileWriter = new RandomAccessFile(file, "rw");

            //覆盖文件内容
            fileWriter.setLength(0);
            FileChannel channel = fileWriter.getChannel();
            Writer writer = Channels.newWriter(channel, "UTF-8");
            writer.write(body);
            writer.flush();

            execStatus = true;
            return execStatus;
        } catch (Exception e) {
            logger.error("存储Key 文件失败，fileName:" + file.getName(), e);
        } finally {
            StreamUtils.close(fileWriter);
            if (!execStatus) { //如果执行失败，把新创建的文件删除
                if (file.exists()) {
                    file.delete();
                }
            }
            file = null;
            fileWriter = null;
        }
        return false;
    }

    /**
     * 从本地文件系统，获取数据
     *
     * @param dataFile 读取数据文件
     * @param clazz    解析数据类定义
     * @param <T>
     * @return
     */
    private <T> List<T> readData(File dataFile, Class<T> clazz) {
        if (dataFile == null) {
            return null;
        }
        if (clazz == null) {
            return null;
        }
        List<T> dataList = new ArrayList<T>();
        //获取给定文件下所有子文件
        List<File> files = getChildFiles(dataFile);
        for (File file : files) {
            RandomAccessFile fileReader = null;
            BufferedReader bufferedReader = null;
            boolean isValid = false;
            try {

                fileReader = new RandomAccessFile(file, "rw");
                FileChannel channel = fileReader.getChannel();
                //所有数据默认UTF-8编码
                Reader reader = Channels.newReader(channel, "UTF-8");

                bufferedReader = new BufferedReader(reader);

                String data = StreamUtils.readFully(bufferedReader);

                if (data == null || data.trim().length() == 0) {
                    logger.error("为什么会有为空的文件？fileName:" + file.getName());
                    continue;
                }
                if (clazz.equals(String.class)) {
                    dataList.add((T) data);
                } else {

                    Gson gson = new GsonBuilder().setDateFormat(DATE_FORMAT).create();
                    Type type = TypeToken.get(clazz).getType();
                    T msg = (T) gson.fromJson(data, type);
                    dataList.add(msg);
                }
                isValid = true;
            } catch (Exception e) {
                logger.error("load data from file error,", e);
                System.out.println(e);
            } finally {
                //Close Stream
                StreamUtils.close(fileReader);
                StreamUtils.close(bufferedReader);

                if (!isValid) {
                    if (file.exists()) {
                        deleteData(file);
                    }
                }
            }
        }
        return dataList;
    }

    /**
     * 获取当前组(group) 所在文件目录
     *
     * @param group 组名
     * @return 组文件目录
     */
    private File getGroupCurrentFile(String group) {
        String groupName = getGroupFileName(group);
        return new File(dataPath, groupName);
    }

    /**
     * 获取 Key 数据存储文件
     *
     * @param key   key
     * @param group key 所属 group
     * @return key 数据存储文件
     */
    private File getKeyFile(String key, String group) {

        File groupFile = getGroupCurrentFile(group);

        String keyFileName = getKeyFileName(key);

        return new File(groupFile, keyFileName);
    }

    /**
     * 获取group 文件名
     *
     * @param group
     * @return group 文件名
     */
    private String getGroupFileName(String group) {
        return DATA_NAME + File.separator + group;
    }

    /**
     * 获取KEY 存储文件名称
     * Key 对象数据存储文件方案：
     * Key数据文件使用  256 * 256 两层目录 hash 存储
     *
     * @param key
     * @return key file name
     */
    private String getKeyFileName(String key) {
        //key hash
        int hash = key.hashCode();

        // key 文件存储使用 256 * 256 两层目录进行 hash 存储
        //Level 0 data dir   hash high bit
        String first = Integer.toString(hash >> 8 & 0xff, 16);

        //Level 1 data dir  hash low bit
        String second = Integer.toHexString(hash & 0xff);
        String fileName = formatLevelName(first) + File.separator + formatLevelName(second) + File.separator + MD5Util.md5Hex(key);

        return fileName;
    }

    //格式化key文件 存储目录名称
    private String formatLevelName(String name) {
        return name.length() == 1 ? "0" + name : name;
    }

    /**
     * 获取文件目录下所有文件
     *
     * @param file 父目录
     * @return 子文件列表
     */
    private List<File> getChildFiles(File file) {
        List<File> files = new ArrayList<File>();

        if (file != null && file.isFile()) {
            files.add(file);
        } else if (file != null && file.isDirectory()) {

            File[] tmpFiles = file.listFiles();

            if (tmpFiles != null && tmpFiles.length > 0) {

                for (File file1 : tmpFiles) {
                    files.addAll(getChildFiles(file1));
                }
            }
        }
        return files;
    }

    /**
     * 关闭数据库
     */
    public void close() {
        dbLock.release();
    }
}
