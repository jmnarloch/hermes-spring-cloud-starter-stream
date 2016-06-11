/**
 * Copyright (c) 2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jmnarloch.spring.cloud.stream.binder.hermes;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import pl.allegro.tech.hermes.client.HermesClient;

/**
 * Hermes client binder.
 *
 * @author Jakub Narloch
 */
public class HermesClientBinder extends AbstractBinder<MessageChannel, HermesConsumerProperties, HermesProducerProperties> {

    private static final String BEAN_NAME_TEMPLATE = "outbound.%s";

    private final HermesClient hermesClient;

    public HermesClientBinder(HermesClient hermesClient) {
        Assert.notNull(hermesClient, "Parameter 'hermesClient' can not be null.");
        this.hermesClient = hermesClient;
    }

    @Override
    protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputTarget, HermesConsumerProperties properties) {
        // or unsupported operation exception
        return null;
    }

    @Override
    protected Binding<MessageChannel> doBindProducer(String name, MessageChannel channel, HermesProducerProperties properties) {

        Assert.isInstanceOf(SubscribableChannel.class, channel);

        logger.debug("Binding Hermes client to topic " + name);
        final MessageHandler handler = new HermesSendingHandler(name);
        final EventDrivenConsumer consumer = createConsumer(name, (SubscribableChannel) channel, handler);
        consumer.start();
        return toBinding(name, channel, consumer);
    }

    private EventDrivenConsumer createConsumer(String name, SubscribableChannel channel, MessageHandler handler) {
        EventDrivenConsumer consumer = new EventDrivenConsumer(channel, handler);
        consumer.setBeanFactory(this.getBeanFactory());
        consumer.setBeanName(String.format(BEAN_NAME_TEMPLATE, name));
        consumer.afterPropertiesSet();
        return consumer;
    }

    private DefaultBinding<MessageChannel> toBinding(String name, MessageChannel channel, EventDrivenConsumer consumer) {
        return new DefaultBinding<>(name, null, channel, consumer);
    }

    private class HermesSendingHandler extends AbstractMessageHandler {

        private final String topic;

        HermesSendingHandler(String topic) {
            Assert.hasLength(topic);
            this.topic = topic;
        }

        @Override
        protected void handleMessageInternal(Message<?> message) throws Exception {
            if (!isValid(message)) {
                return;
            }
            publish(message);
        }

        private void publish(Message<?> message) {
            hermesClient.publish(
                    topic,
                    getContentType(message),
                    getPayloadAsBytes(message)
            );
        }

        private boolean isValid(Message<?> message) {
            if (getContentType(message) == null) {
                logger.warn("The message content type hasn't been set");
                return false;
            } else if (!(message.getPayload() instanceof byte[])) {
                logger.warn("The message payload must be a byte array");
                return false;
            }
            return true;
        }

        private String getContentType(Message<?> message) {
            Object contentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
            if (contentType != null) {
                return String.valueOf(contentType);
            }
            return null;
        }

        private byte[] getPayloadAsBytes(Message<?> message) {
            if (message.getPayload() instanceof byte[]) {
                return (byte[]) message.getPayload();
            }
            logger.warn("The message payload must be ");
            return null;
        }
    }
}
