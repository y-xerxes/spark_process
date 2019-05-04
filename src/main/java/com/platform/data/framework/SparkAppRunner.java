package com.platform.data.framework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.collect.ImmutableMap;
import com.platform.data.framework.config.*;
import com.platform.data.framework.helper.DatabaseConfigManager;
import com.platform.data.framework.helper.LocalhostMysqlJdbcHelper;
import com.platform.data.framework.utils.HttpHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.coyote.http2.Http2Error;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.BeansException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Slf4j
public class SparkAppRunner implements CommandLineRunner, ApplicationContextAware {

    private final String COMMAND_RUN = "run";
    private final String COMMAND_STREAM = "stream";

    private SparkDevelopmentConfig sparkDevelopmentConfig;
    private HdfsConfig hdfsConfig;
    private DatabaseConfigGroup databaseConfigGroup;
    private LivyConfig livyConfig;
    private KafkaConfig kafkaConfig;
    private ZkConfig zkConfig;
    private ApplicationContext applicationContext;
    private final Map<String, AbstractSparkApp> sparkApps = new HashMap<>();
    private final Map<String, AbstractSparkStreamApp> sparkStreamApps = new HashMap<>();

    public SparkAppRunner(SparkDevelopmentConfig sparkDevelopmentConfig,
                          HdfsConfig hdfsConfig,
                          KafkaConfig kafkaConfig,
                          DatabaseConfigGroup databaseConfigGroup,
                          LivyConfig livyConfig,
                          ZkConfig zkConfig) {
        this.sparkDevelopmentConfig = sparkDevelopmentConfig;
        this.hdfsConfig = hdfsConfig;
        this.databaseConfigGroup = databaseConfigGroup;
        this.livyConfig = livyConfig;
        this.kafkaConfig = kafkaConfig;
        this.zkConfig = zkConfig;
    }

    @Override
    public void run(String... args) throws Exception {
        List<String> commandLineArgs = new ArrayList<>(Arrays.asList(args));
        log.info("总体启动参数：{}", commandLineArgs);

        if (commandLineArgs.size() == 0) {
            // 这种情况, 往往是在开发, 直接根据配置调用, 启动对应的spark app, local mode
            String runAppName = sparkDevelopmentConfig.getRunAppName();
            List<String> runArgs = sparkDevelopmentConfig.getRunArgs();

            this.executeApp(runAppName, runArgs);

        } else {
            assert commandLineArgs.size() > 1;
            String command = commandLineArgs.get(0);

            commandLineArgs.remove(0);

            if (command.equalsIgnoreCase(COMMAND_STREAM)) {
            String appName = commandLineArgs.get(0);
            commandLineArgs.remove(0);
            executeStreamApp(appName, commandLineArgs);
            } else {
                log.error("未知命令： {}", command);
            }
        }
    }


    private void executeApp(String runAppName, List<String> runArgs) {
        try {
            AbstractSparkApp sparkApp = sparkApps.get(runAppName);
            if (sparkApp == null) {
                log.error("尝试运行应用: {}, 当前上下文中没有找到!", runAppName);
            } else {
                this.sparkDevelopmentConfig.setAppName(runAppName);
                sparkApp.updateSparkContext(
                        sparkSession(this.sparkDevelopmentConfig),
                        this.hdfsConfig,
                        this.kafkaConfig,
                        this.zkConfig);
                        sparkApp.run(runArgs);
            }
        } catch (RuntimeException re) {
            log.error(String.format(String.format("运行应用%s 失败", runAppName), re));
            throw re;
        }
    }


    private void executeStreamApp(String runAppName, List<String> runArgs) throws Exception {
        log.info("当前参数列表: {}", runArgs);

        String jarName = runArgs.get(0);
        String mainKlass = runArgs.get(1);
        String driverMemory = runArgs.get(2);
        String executorMemory = runArgs.get(3);
        String cpuSize = runArgs.get(4);

        String LIVY_HOST = this.livyConfig.getLivyAddress();
        log.info("当前livy_address: {}", LIVY_HOST);

        ArrayList<String> runArgsList = new ArrayList<>(3);

        Map<String, Object> envData = new HashMap<>(5);
        envData.put("spark", this.sparkDevelopmentConfig);
        envData.put("hdfs", this.hdfsConfig);
        envData.put("db", this.databaseConfigGroup);
        envData.put("kafka", this.kafkaConfig);
        envData.put("zookeeper", this.zkConfig);

        Map<String, Object> joowingEnv = ImmutableMap.of("xerxes", envData);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategy.KebabCaseStrategy());
        String envDataArgs = objectMapper.writeValueAsString(joowingEnv);

        runArgsList.add("run");
        runArgsList.add(envDataArgs);
        runArgsList.add(runAppName);

        Map<String, Object> jsonBody = new HashMap<>(6);

        Map<String, String> sparkConfig = ImmutableMap.of(
                "spark.cores.max", cpuSize, "spark.streaming.kafka.maxRatePerPartition", "100000");

        jsonBody.put("className",mainKlass);
        jsonBody.put("driverMemory",driverMemory);
        jsonBody.put("executorMemory",executorMemory);
        jsonBody.put("conf",sparkConfig);
        jsonBody.put("args",runArgsList);
        jsonBody.put("file",""+jarName);

        String submitJob = objectMapper.writeValueAsString(jsonBody);
        log.info("当前submitJob: {}", submitJob);
//        发送post请求之前,先删除之前启动的livy　session,再去发送post请求
        LocalhostMysqlJdbcHelper localhostMysqlJdbcHelper = new LocalhostMysqlJdbcHelper(new DatabaseConfigManager(this.databaseConfigGroup));
        List<Map<String, Object>> map = localhostMysqlJdbcHelper.executeSQLWithStatement(
                "database_service_maintenance",
                "select * from `livy_streaming_tasks` where `streaming_task_name` = ?",
                preparedStatement -> preparedStatement.setString(1, runAppName)
        );

        log.info("maps: {}", map);
        if(map.size() > 0) {
            map.forEach(livyStreamingTasksMap -> {
                try {
                    int streaming_batch_id = (Integer) livyStreamingTasksMap.get("streaming_batch_id");
                    HttpHelper.sendDeleteReq("http://" + LIVY_HOST + "/batches/" + streaming_batch_id);
                    localhostMysqlJdbcHelper.executeSQLWithStatement(
                            "database_service_maintenance",
                            "delete from `livy_streaming_tasks` where `streaming_batch_id` = ?",
                            preparedStatement -> preparedStatement.setInt(1, streaming_batch_id)
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        String reqResult = HttpHelper.sendPostDataByJson("http://" + LIVY_HOST + "/batches", submitJob);
        log.info("当前reqResult: {}", reqResult);

        JsonNode node = objectMapper.readTree(reqResult);

        String batchId = node.get("id").toString();
        log.info("post请求batchId: {}", batchId);

        String state = node.get("state").toString().replace("\"","");
        log.info("post请求state: {}", state);

        log.info("post发送后将其batchId:{}和AppName:{}记录到livy_streaming_tasks表中", batchId, runAppName);

        localhostMysqlJdbcHelper.executeSQLWithStatement(
                "database_service_maintenance",
                "insert into `livy_streaming_tasks` (`streaming_batch_id`, `streaming_task_name`) VALUES (?, ?)",
                preparedStatement -> {
                    preparedStatement.setInt(1, Integer.parseInt(batchId));
                    preparedStatement.setString(2, runAppName);
                }
        );
        String run_state = "running";
        while (run_state.equalsIgnoreCase("running") ){

            Thread.sleep(5000);

            String getResult = HttpHelper.sendGetReq("http://" + LIVY_HOST + "/batches/" + batchId + "/state");
            if (getResult != null) {
                run_state = objectMapper.readTree(getResult).get("state").toString().replace("\"","");
            }
            log.info("5秒后请求state: {}", run_state);
        }
        if (!"success".equalsIgnoreCase(run_state)) {
            throw new Exception("Livy job run into state: "+ run_state + ", and batch id is: " + batchId);
        }
    }


    public SparkSession sparkSession(SparkDevelopmentConfig sparkDevelopmentConfig) {

        if (SparkSession.getActiveSession().isEmpty()) {
            log.info("新建sparkSession");
            return SparkSession.builder()
                    .config(sparkConf(sparkDevelopmentConfig))
                    .appName(sparkDevelopmentConfig.getAppName())
                    .enableHiveSupport()
                    .getOrCreate();
        } else {
            log.info("使用内置的spark");
            return SparkSession.getActiveSession().get();
        }
    }

    private SparkConf sparkConf(SparkDevelopmentConfig sparkConfig) {

        SparkConf sparkConf = new SparkConf(true);
        sparkConf.setAppName(sparkConfig.getAppName());
        sparkConf.set("spark.sql.parquet.enableVectorizedReader", "true");
        sparkConf.set("spark.sql.warehouse.dir", "/user/hive/warehouse");

        try {
//            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
//            InputStream hiveInputStream =
//                    FileUtils.openInputStream(new File("dev_hadoop_configs/hive-site.xml"));
//            InputStream coreSiteStream =
//                    FileUtils.openInputStream(new File("dev_hadoop_configs/core-site.xml"));
//            InputStream hdfsSiteStream =
//                    FileUtils.openInputStream(new File("dev_hadoop_configs/hdfs-site.xml"));
//            configuration.addResource(hiveInputStream);
//            configuration.addResource(coreSiteStream);
//            configuration.addResource(hdfsSiteStream);
//
//            configuration.iterator().forEachRemaining(c -> {
//                log.info("{} -> {}", c.getKey(), c.getValue());
//                sparkConf.set(c.getKey(), c.getValue());
//            });

            sparkConf.setSparkHome(sparkConfig.getSparkHome())
                    .setMaster(sparkConfig.getMasterUrl());
//        } catch (IOException e) {
        } catch (Exception e) {
            log.error("载入开发环境Hadoop配置有问题的有问题, 本地开发的话需要关注, 线上环境请忽略");
        }

        return sparkConf;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;

        this.sparkApps.clear();
        Map<String, AbstractSparkApp> currentSparkApps = this.applicationContext.getBeansOfType(AbstractSparkApp.class);
        Map<String, AbstractSparkStreamApp> currentSparkStreamApps = this.applicationContext.getBeansOfType(AbstractSparkStreamApp.class);
        currentSparkApps.values().forEach(sparkApp -> sparkApps.put(sparkApp.appName(), sparkApp));
        currentSparkStreamApps.values().forEach(sparkStreamApp -> sparkStreamApps.put(sparkStreamApp.appName(), sparkStreamApp));
    }
}
