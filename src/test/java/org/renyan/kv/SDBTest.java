package org.renyan.kv;


import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: weirenan
 * Date: 14-5-16
 * Time: 下午10:53
 */
public class SDBTest {

    @Test
    public void testOpen() throws IOException {
        SDB sdb = SDBFactory.open("d:\\sdb");

        sdb.close();
    }

    @Test
    public void testRepeatOpen() throws IOException {
        SDB sdb = SDBFactory.open("d:\\sdb");

        sdb = SDBFactory.open("d:\\sdb");

        sdb.close();
    }

    @Test
    public void putData() throws IOException {
        SDB sdb = SDBFactory.open("d:\\sdb");

        sdb.put("hello", "hello", "hello\nworld");

        String s = sdb.get("hello", "hello", String.class);

        assertEquals("hello\nworld", s);

        System.out.println(s);

        sdb.close();
    }

    @Test
    public void putDate() throws IOException {
        SDB sdb = SDBFactory.open("d:\\sdb");

        Date date = new Date();
        sdb.put("hello_date", "hello", date);

        Date s = sdb.get("hello_date", "hello", Date.class);

        assertEquals(date, s);

        System.out.println(s);

        sdb.close();
    }
}
