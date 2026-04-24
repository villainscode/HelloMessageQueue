package net.harunote.hellomessagequeue.step7;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 재시도 카운트는 "메시지 헤더(x-retry-count)"에 실어 메시지와 함께 이동시킨다.
 *
 * [이전 구현의 문제점]
 *  - retryCount 를 인스턴스 필드로 두면 @Component(싱글톤) 특성상
 *    모든 메시지가 같은 카운터를 공유해버려 메시지별 재시도 추적이 불가능하다.
 *    (메시지 A 실패 중에 메시지 B 가 성공하면 A 의 카운터가 0으로 초기화됨)
 *  - 여러 스레드가 동시에 listener 를 호출할 경우 int 필드에 대한
 *    read/write 가 원자적이지 않아 race condition 이 발생한다.
 *  - basicReject(tag, true) 로 requeue 해도 RabbitMQ 는 헤더를 갱신해주지 않으므로
 *    "몇 번째 재시도인지" 를 메시지 자신이 알 수 없다.
 *
 * [수정된 구조]
 *  - 실패 시 현재 메시지는 basicAck 로 큐에서 제거하고,
 *    x-retry-count 를 1 증가시킨 "새 메시지" 를 같은 exchange 로 재발행한다.
 *  - MAX_RETRIES 를 초과하면 basicNack(requeue=false) 로 거부 → DLX → DLQ 로 보낸다.
 */
@Component
public class OrderConsumer {
    private static final int MAX_RETRIES = 3;
    private static final String RETRY_HEADER = "x-retry-count";

    private final RabbitTemplate rabbitTemplate;

    public OrderConsumer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = RabbitMQConfig.ORDER_COMPLETED_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void processOrder(Message message, Channel channel, @Header("amqp_deliveryTag") long tag) {
        String body = new String(message.getBody());
        // 첫 수신이면 헤더가 없으므로 0 으로 시작. 재시도 메시지는 증가된 값이 들어있다.
        int retryCount = (int) message.getMessageProperties().getHeaders().getOrDefault(RETRY_HEADER, 0);

        try {
            // 실패 유발
            if ("fail".equalsIgnoreCase(body)) {
                throw new RuntimeException(body);
            }
            // 성공 처리
            System.out.println("# 성공 : " + body);
            channel.basicAck(tag, false);
        } catch (Exception e) {
            System.err.println("# error 발생 : " + e.getMessage());
            handleRetry(channel, tag, body, retryCount);
        }
    }

    private void handleRetry(Channel channel, long tag, String body, int retryCount) {
        try {
            if (retryCount < MAX_RETRIES) {
                // 현재 delivery 는 ack 로 정리하고, 카운트 +1 한 새 메시지를 재발행한다.
                // requeue(basicReject/Nack with requeue=true) 가 아닌 재발행을 쓰는 이유는
                // 헤더를 갱신해야 메시지 자신이 재시도 횟수를 들고 다닐 수 있기 때문.
                System.err.println("#### Fail & Retry = " + retryCount);
                channel.basicAck(tag, false);

                Message retryMessage = MessageBuilder.withBody(body.getBytes())
                        .setHeader(RETRY_HEADER, retryCount + 1)
                        .build();
                rabbitTemplate.send(
                        RabbitMQConfig.ORDER_EXCHANGE,
                        "order.completed.shipping",
                        retryMessage
                );
            } else {
                // 최대 재시도 초과 → requeue=false 로 거부하면 큐에 설정된 DLX 로 라우팅되어 DLQ 에 쌓인다.
                System.err.println("#### 최대 횟수 초과, DLQ 이동 시킴 ");
                channel.basicNack(tag, false, false);
            }
        } catch (IOException e1) {
            System.err.println("# fail & reject message : " + e1.getMessage());
        }
    }
}
