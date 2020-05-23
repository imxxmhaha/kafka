package cn.xxm.kafka.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTopicTests {

    public final static String TOPIC_NAME = "xxm_topic";

    /**
     * 创建topic
     */
    @Test
    public void topicTest() {
        createTopic();
    }


    /**
     * 获取topic列表
     */
    @Test
    public void topicList() throws Exception {
        AdminClient adminClient = getAdminClient();
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult topicsResultList = adminClient.listTopics(options);
        Set<String> topicNameList = topicsResultList.names().get();


        Collection<TopicListing> topicListings = topicsResultList.listings().get();

        Map<String, TopicListing> stringTopicListingMap = topicsResultList.namesToListings().get();

        topicNameList.forEach(System.out::println);
        topicListings.forEach(item -> {
            System.out.println(item);
        });
        System.out.println("stringTopicListingMap = " + stringTopicListingMap);

    }


    /**
     * topic描述信息
     */
    @Test
    public void descriptTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        System.out.println("stringTopicDescriptionMap = " + stringTopicDescriptionMap);
    }

    /**
     * topic配置信息
     */
    @Test
    public void descriptConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));

        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();

        System.out.println("configResourceConfigMap = " + configResourceConfigMap);

    }

    /**
     * 修改topic配置信息
     */
    @Test
    public void alertConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient();
        Map<ConfigResource, Config> configMap = new HashMap<>();

        // 组织两个参数
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "false")));
        configMap.put(configResource, config);

        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configMap);
        alterConfigsResult.all().get();
    }


    /**
     * 删除topic
     */
    @Test
    public void delTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        Void aVoid = deleteTopicsResult.all().get();
    }


    /**
     * 增加Partitions 数量
     */
    @Test
    public void incrPartitions() throws ExecutionException, InterruptedException {
        AdminClient adminClient = getAdminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>();

        NewPartitions newPartitions = NewPartitions.increaseTo(2);
        partitionsMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(partitionsMap);
        partitions.all().get();

    }


    private void createTopic() {
        AdminClient adminClient = getAdminClient();
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("topics = " + JSONObject.toJSONString(topics));
    }


    private AdminClient getAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "114.67.203.88:9092");
        return AdminClient.create(properties);
    }

}
