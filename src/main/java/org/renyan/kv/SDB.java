package org.renyan.kv;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: weirenan
 * Date: 14-5-16
 * Time: 下午9:17
 */
public interface SDB {

    public <T> boolean put(String key, String group, T body);

    public <T> T get(String key, String group, Class<T> clazz);

    public <T> List<T> list(String group, Class<T> clazz);

    public boolean delete(String key, String group);

    public void close();
}
