package com.sandbox.javachallenge.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*


class KafkaIntegrationTestApplicationContextInitializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
    override fun initialize(applicationContext: ConfigurableApplicationContext) {
        TestPropertyValues.of(
            "spring.kafka.bootstrap-servers=${KafkaIntegrationTest.kafkaContainer.bootstrapServers}"
        ).applyTo(applicationContext.environment)
    }
}

@SpringBootTest
@ExtendWith(SpringExtension::class)
// @EmbeddedKafka(partitions = 1, topics = ["test_topic"], brokerProperties = ["listeners=PLAINTEXT://localhost:9092", "port=9092"])
@ContextConfiguration(initializers = [KafkaIntegrationTestApplicationContextInitializer::class])
class KafkaIntegrationTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    companion object {

        public lateinit var kafkaContainer: KafkaContainer

        @BeforeAll
        @JvmStatic
        fun setUp() {
            kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            kafkaContainer.start()

            // Create topic
            createTopic("test_topic", 1, 1)
        }

        private fun createTopic(topic: String, numPartitions: Int, replicationFactor: Short) {
            val config = mapOf(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers
            )

            AdminClient.create(config).use { adminClient ->
                val newTopic = NewTopic(topic, numPartitions, replicationFactor)
                adminClient.createTopics(listOf(newTopic)).all().get()
            }
        }

        fun createConsumerProps(): Map<String, Any> {
            val config = HashMap<String, Any>()
            config[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
            config[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
            config[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            config[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            return config
        }
    }

    @Test
    fun `test kafka communication`() {
        // Given
        val message = "Hello, Kafka"
        val topic = "test_topic"

        // When
        // Produce a message to the topic
        kafkaTemplate.send(topic, message)

        // Then
        // Create a Kafka consumer to consume the message
        val consumer = KafkaConsumer<String, String>(createConsumerProps())
        consumer.subscribe(listOf(topic))

        val records = consumer.poll(Duration.ofSeconds(10))

        if (records.isEmpty) {
            throw AssertionError("No messages were received")
        }

        for (record in records) {
            assertEquals(message, record.value())
        }

        consumer.close()
    }
}