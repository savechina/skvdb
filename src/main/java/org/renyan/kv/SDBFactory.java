package org.renyan.kv;

import org.renyan.kv.impl.SDBImpl;

import java.io.File;
import java.io.IOException;

/**
 * User: weirenan
 * Date: 14-5-16
 * Time: 下午10:27
 */
public class SDBFactory {
    private static SDB sdb;

    /**
     * 打开一个DB 实例
     *
     * @param filename
     * @return
     * @throws IOException
     */
    public static SDB open(String filename) throws IOException {
        if (sdb == null) {
            synchronized (SDBFactory.class) {
                if ((sdb == null)) {
                    sdb = new SDBImpl(filename);
                }
            }
        }
        return sdb;
    }
}
