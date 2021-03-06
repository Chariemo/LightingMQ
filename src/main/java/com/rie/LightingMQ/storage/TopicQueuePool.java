package com.rie.LightingMQ.storage;

import com.rie.LightingMQ.config.ServerConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 1. block file named: block_(topicName)_(fileNo).lmq_data
 * 2. index file named: index_(topicName).lmq_index
 * Created by Charley on 2017/7/23.
 */
public class TopicQueuePool {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);
    public static final String DEFAULT_DATA_PATH = "data";
    private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingQueue<>();
    private static TopicQueuePool TQPINSTANCE;
    private final String dataDir;
    private Map<String, TopicQueue> queueMap;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {

            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        }
    });

    private TopicQueuePool(ServerConfig serverConfig) {

        if (StringUtils.isBlank(serverConfig.getDataDir())) {
            this.dataDir = DEFAULT_DATA_PATH;
        }
        else {
            this.dataDir = serverConfig.getDataDir();
        }

        File fileDir = new File(dataDir);
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        if (!fileDir.isDirectory() || !fileDir.canRead()) {
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable dir");
        }

        //扫描该目录下的所有index文件 获取相应topic queue
        this.queueMap = scanDir(fileDir);

        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {

                for (TopicQueue topicQueue : queueMap.values()) {
                    topicQueue.sync();
                }
                deleteOldBlockFile();
            }
        }, 1000L, 1000L, TimeUnit.MILLISECONDS);

    }

    public Map<String, TopicQueue> scanDir(File fileDir) {

        Map<String, TopicQueue> tempTQMap = new HashMap<>();
        File[] indexFiles = fileDir.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return isIndexFile(name);
            }
        });

        for (File indexFile : indexFiles) {
            String queueName = getQueueNameFromIndex(indexFile.getName());
            tempTQMap.put(queueName, new TopicQueue(queueName, fileDir.getAbsolutePath()));
        }

        return tempTQMap;
    }

    public boolean isIndexFile(String fileName) {

        return fileName.endsWith(IndexImpl.INDEX_FILE_SUFFIX);
    }

    public String getQueueNameFromIndex(String indexName) {

        String queueName = indexName.substring(0, indexName.indexOf('.'));
        return queueName.split("_")[1];
    }

    // 删除已读数据块
    public void deleteOldBlockFile() {

        String oldFilePath = DELETING_QUEUE.poll();
        if (StringUtils.isNotBlank(oldFilePath)) {
            File oldFile = new File(oldFilePath);
            if (getTimeFromLastMod(oldFile) > 48) { // 已读数据块最后修改时间超过48h 删除
                oldFile.setReadable(false);
                if (!oldFile.delete()) {
                    LOGGER.warn("block file: {} delete failed.", oldFilePath);
                }
            }
        }
    }

    private long getTimeFromLastMod(File file) {

        long currentTime = System.currentTimeMillis();
        long lastModifyTime = file.lastModified();
        return ((currentTime - lastModifyTime) / (1000 * 60 * 60));

    }

    private void shutdown() {

        this.scheduler.shutdown();
        for (TopicQueue topicQueue : this.queueMap.values()) {
            topicQueue.close();
        }
        while (!DELETING_QUEUE.isEmpty()) {
            deleteOldBlockFile();
        }
    }

    public synchronized static void singletonInstance(ServerConfig config) {

        if (TQPINSTANCE == null) {
            TQPINSTANCE = new TopicQueuePool(config);
        }
    }

    public synchronized static void singletonInstance(String configPath) {

        singletonInstance(new ServerConfig(configPath));
    }

    public static void toClear(String dataFilePath) {

        try {
            DELETING_QUEUE.put(dataFilePath);
        } catch (InterruptedException e) {
            LOGGER.error("add oldDataBlockFile {} to delete_queue failed", dataFilePath);
        }
    }

    public static void close() {

        if (TQPINSTANCE != null) {
            TQPINSTANCE.shutdown();
            TQPINSTANCE = null;
        }
    }

    // 从queuePool中获取topicQueue
    public synchronized static TopicQueue getTopicQueue(String topicName) {

        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException();
        }
        if (TQPINSTANCE == null || !TQPINSTANCE.queueMap.containsKey(topicName)) {
            return null;
        }
        return TQPINSTANCE.queueMap.get(topicName);
    }

    //从queuePool中获取topicQueue 不存在则创建
    public synchronized static TopicQueue getOrCreateTopicQueue(String topicName) {

        TopicQueue topicQueue = getTopicQueue(topicName);
        if (topicQueue == null) {
            topicQueue = new TopicQueue(topicName, TQPINSTANCE.dataDir);
            TQPINSTANCE.queueMap.put(topicName, topicQueue);
        }
        return topicQueue;
    }

}
