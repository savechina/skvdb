package org.renyan.kv.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.renyan.kv.util.StreamUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static java.lang.String.format;

public class DBLock {
    private final File lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    public DBLock(File lockFile)
            throws IOException {
        Preconditions.checkNotNull(lockFile, "lockFile is null");
        this.lockFile = lockFile;

        // open and lock the file
        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        try {
            lock = channel.tryLock();
        } catch (IOException e) {
            StreamUtils.close(channel);
            throw e;
        }

        if (lock == null) {
            throw new IOException(format("Unable to acquire lock on '%s'", lockFile.getAbsolutePath()));
        }
    }

    public boolean isValid() {
        return lock.isValid();
    }

    public void release() {
        try {
            lock.release();
        } catch (IOException e) {
            Throwables.propagate(e);
        } finally {
            StreamUtils.close(channel);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DBLock");
        sb.append("{lockFile=").append(lockFile);
        sb.append(", lock=").append(lock);
        sb.append('}');
        return sb.toString();
    }
}