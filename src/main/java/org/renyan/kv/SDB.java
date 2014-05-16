package org.renyan.kv;

import java.util.List;

/**
 * Simple key value db
 * <p/>
 * key by group store
 * User: weirenan
 * Date: 14-5-16
 * Time: 下午9:17
 */
public interface SDB {

    /**
     * save  key value
     *
     * @param key   key
     * @param group key is group
     * @param body  value
     * @param <T>   value Type
     * @return
     */
    public <T> boolean put(String key, String group, T body);

    /**
     * give key and group return this value
     *
     * @param key   key
     * @param group key is group
     * @param clazz value is class type
     * @param <T>
     * @return return value
     */
    public <T> T get(String key, String group, Class<T> clazz);

    /**
     * query give group all key's values
     *
     * @param group group
     * @param clazz type
     * @param <T>
     * @return
     */
    public <T> List<T> list(String group, Class<T> clazz);

    /**
     * delete give key's value
     *
     * @param key   key
     * @param group group
     * @return
     */
    public boolean delete(String key, String group);

    /**
     * close db release lock
     */
    public void close();
}
