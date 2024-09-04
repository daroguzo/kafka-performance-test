import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KafkaPerformanceTest {

    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage for Producer: java KafkaPerformanceTest p <broker-address> <topic-name> <num-messages> <throughput> <message-size>");
            System.out.println("Usage for Consumer: java KafkaPerformanceTest c <broker-address> <topic-name> <consumer-id> <consumer-group>");
            System.exit(1);
        }

        String mode = args[0]; // 모드: p 또는 c

        if (mode.equalsIgnoreCase("p")) {
            // 프로듀서 실행 변수 확인
            if (args.length != 6) {
                System.out.println("Usage for Producer: java KafkaPerformanceTest p <broker-address> <topic-name> <num-messages> <throughput> <message-size>");
                System.exit(1);
            }

            String brokerAddress = args[1]; // Kafka 브로커 주소
            String topicName = args[2];     // Kafka 토픽 이름
            int numMessages = Integer.parseInt(args[3]); // 총 메시지 개수
            int throughput = Integer.parseInt(args[4]);  // 초당 메시지 전송 개수
            int messageSize = Integer.parseInt(args[5]); // 메시지 크기 (바이트 단위)

            runProducer(brokerAddress, topicName, numMessages, throughput, messageSize);

        } else if (mode.equalsIgnoreCase("c")) {
            // 컨슈머 실행 변수 확인
            if (args.length != 5) {
                System.out.println("Usage for Consumer: java KafkaPerformanceTest c <broker-address> <topic-name> <consumer-id> <consumer-group>");
                System.exit(1);
            }

            String brokerAddress = args[1]; // Kafka 브로커 주소
            String topicName = args[2];     // Kafka 토픽 이름
            String consumerID = args[3];    // 컨슈머 ID
            String consumerGroup = args[4]; // 컨슈머 그룹

            runConsumer(brokerAddress, topicName, consumerID, consumerGroup);

        } else {
            System.out.println("Invalid mode. Use 'p' for producer or 'c' for consumer.");
            System.exit(1);
        }
    }

    // Kafka Producer 메서드
    private static void runProducer(String brokerAddress, String topicName, int numMessages, int throughput, int messageSize) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 메시지 내용 생성 (Java 8 호환)
        String messageContent = generateMessageContent(messageSize);

        int delay = 1000 / throughput; // 밀리초 단위로 메시지 전송 간격 계산

        try (FileWriter producerLogFile = new FileWriter("produced_messages.log")) {
            for (int i = 0; i < numMessages; i++) {
                String topicPartition = String.format("%-20s", topicName + "-0"); // 토픽 파티션 이름을 20자리로 고정
                String consumerID = String.format("%-10s", "N/A"); // ConsumerID는 프로듀서 단계에서 "N/A"로 채움
                String uniqueNumber = String.format("%010d", i); // 유니크 번호를 10자리로 고정
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                String consumeTime = String.format("%-23s", "N/A"); // ConsumeTime은 프로듀서 단계에서 "N/A"로 채움

                String logEntry = String.format("%s %s %s %s %s %s%n",
                    topicPartition, consumerID, uniqueNumber, timestamp, consumeTime, padRight(messageContent, messageSize));

                // Kafka로 전송 전에 메시지를 로그 파일에 기록
                producerLogFile.write(logEntry);

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, logEntry);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(delay); // 메시지 전송 속도 제어
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    // 메시지 내용을 생성하는 메서드 (Java 8 호환)
    private static String generateMessageContent(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('x'); // 지정된 크기만큼 'x'로 채움
        }
        return sb.toString();
    }

    // 패딩 처리를 위해 문자열의 오른쪽을 채우는 메서드
    private static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s); // n만큼 공간을 확보하고 왼쪽 정렬
    }

    // Kafka Consumer 메서드
    private static void runConsumer(String brokerAddress, String topicName, String consumerID, String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        try (FileWriter consumerLogFile = new FileWriter("consumed_messages.log")) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // String produceTimestamp = record.value().split(" ")[3]; // 생산 시간 추출
                    String consumeTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    // String[] logComponents = record.value().split(" ", 6);

                    byte[] valueBytes = record.value().getBytes();

                    // 컨슈머 ID 기입
                    System.arraycopy(String.format("%-10s", consumerID).getBytes(), 0, valueBytes, 21, 10);
                    // ConsumeTime 기입
                    System.arraycopy(consumeTimestamp.getBytes(), 0, valueBytes, 67, 23);

                    // String logEntry = String.format("%s %s %s %s %s %s%n",
                    //     String.format("%-20s", logComponents[0]), // TopicPartition
                    //     String.format("%-10s", consumerID), // ConsumerID를 10자리로 고정
                    //     logComponents[2], // UniqueNumber
                    //     logComponents[3], // ProduceTime
                    //     consumeTimestamp, // ConsumeTime
                    //     padRight(logComponents[5], logComponents[5].length())); // Data

                    String newValue = new String(valueBytes);

                    consumerLogFile.write(newValue); // 파일에 로그 기록
                    consumerLogFile.flush();
                    System.out.print(newValue); // 콘솔 출력
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
