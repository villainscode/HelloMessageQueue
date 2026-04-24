package net.harunote.hellomessagequeue.step8;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    // retry 제거 및 강의 문서자료 업데이트 (2026.04.24)
    @RabbitListener(queues = RabbitMQConfig.ORDER_COMPLETED_QUEUE)
    public void processOrder(String message) {
        System.out.println("Received: " + message);
        if ("fail".equalsIgnoreCase(message)) {
            // 예외가 던져지면 Spring AMQP가 application.yml 의 retry 정책대로 최대 3회 시도
            // 3회 모두 실패하면 default-requeue-rejected: false 설정에 따라 requeue 하지 않고
            // 큐에 설정된 DLX를 타고 DLQ로 이동한다.
            throw new RuntimeException("Processing failed: " + message);
        }
        System.out.println("Processed successfully: " + message);
    }
}