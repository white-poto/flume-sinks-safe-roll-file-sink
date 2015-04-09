package cn.huyanping.flume.sinks;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by Jenner on 2015/4/9.
 *
 * 修正org.apache.flume.formatter.output.PathManager在移动文件时可能出现文件名重复的问题
 */
public class SafePathManager  {
    private long seriesTimestamp;
    private File baseDirectory;
    private AtomicInteger fileIndex;

    private File currentFile;

    public SafePathManager() {
    }

    public File nextFile() {
        //重新生成微秒级文件名
        seriesTimestamp = System.currentTimeMillis();
        fileIndex = new AtomicInteger();
        currentFile = new File(baseDirectory, seriesTimestamp + "-"
                + fileIndex.incrementAndGet());

        return currentFile;
    }

    public File getCurrentFile() {
        if (currentFile == null) {
            return nextFile();
        }

        return currentFile;
    }

    public void rotate() {
        currentFile = null;
    }

    public File getBaseDirectory() {
        return baseDirectory;
    }

    public void setBaseDirectory(File baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    public long getSeriesTimestamp() {
        return seriesTimestamp;
    }

    public AtomicInteger getFileIndex() {
        return fileIndex;
    }
}
