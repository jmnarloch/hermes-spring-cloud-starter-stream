/**
 * Copyright (c) 2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.http.MediaType;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import pl.allegro.tech.hermes.client.HermesClient;
import pl.allegro.tech.hermes.client.HermesMessage;
import pl.allegro.tech.hermes.client.HermesResponse;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.http.MediaType.APPLICATION_JSON;

/**
 * Hermes client binder.
 *
 * @author Jakub Narloch
 */
public class HermesClientBinder
        extends AbstractBinder<MessageChannel, ExtendedConsumerProperties<HermesConsumerProperties>, ExtendedProducerProperties<HermesProducerProperties>>
        implements ExtendedPropertiesBinder<MessageChannel, HermesConsumerProperties, HermesProducerProperties> {

    private static final String BEAN_NAME_TEMPLATE = "outbound.%s";

    private static final MediaType AVRO_BINARY = MediaType.parseMediaType("avro/binary");

    private static final String SCHEMA_VERSION_HEADER = "Schema-Version";

    private final HermesClient hermesClient;

    private HermesExtendedBindingProperties hermesExtendedBindingProperties = new HermesExtendedBindingProperties();

    public HermesClientBinder(HermesClient hermesClient) {
        Assert.notNull(hermesClient, "Parameter 'hermesClient' can not be null.");
        this.hermesClient = hermesClient;
    }

    @Override
    protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputTarget, ExtendedConsumerProperties<HermesConsumerProperties> properties) {
        // unsupported operation
        throw new UnsupportedOperationException("Binding Hermes as a consumer is not supported");
    }

    @Override
    protected Binding<MessageChannel> doBindProducer(String name, MessageChannel channel, ExtendedProducerProperties<HermesProducerProperties> properties) {
        Assert.isInstanceOf(SubscribableChannel.class, channel);

        logger.debug("Binding Hermes client to topic " + name);
        final MessageHandler handler = new HermesSendingHandler(name);
        final EventDrivenConsumer consumer = createConsumer(name, (SubscribableChannel) channel, handler);
        consumer.start();
        return toBinding(name, channel, consumer);
    }

    public void setHermesExtendedBindingProperties(HermesExtendedBindingProperties hermesExtendedBindingProperties) {
        this.hermesExtendedBindingProperties = hermesExtendedBindingProperties;
    }

    @Override
    public HermesConsumerProperties getExtendedConsumerProperties(String channelName) {
        return hermesExtendedBindingProperties.getExtendedConsumerProperties(channelName);
    }

    @Override
    public HermesProducerProperties getExtendedProducerProperties(String channelName) {
        return hermesExtendedBindingProperties.getExtendedProducerProperties(channelName);
    }

    private EventDrivenConsumer createConsumer(String name, SubscribableChannel channel, MessageHandler handler) {
        EventDrivenConsumer consumer = new EventDrivenConsumer(channel, handler);
        consumer.setBeanFactory(getBeanFactory());
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
            validate(message);
            publish(
                    buildHermesMessage(message)
            );
        }

        private void validate(Message<?> message) {
            final Optional<String> contentType = getContentType(message);
            if (!contentType.isPresent()) {
                throw new IllegalStateException("Header 'contentType' is required to publish Hermes message");
            }
        }

        private HermesMessage buildHermesMessage(Message<?> message) {
            final Optional<MediaType> contentType = getContentType(message)
                    .map(MediaType::parseMediaType);

            if (APPLICATION_JSON.isCompatibleWith(contentType.get())) {
                return HermesMessage.hermesMessage(topic, getPayloadAsBytes(message))
                        .json()
                        .build();
            } else if (AVRO_BINARY.isCompatibleWith(contentType.get())) {
                return HermesMessage.hermesMessage(topic, getPayloadAsBytes(message))
                        .avro(getSchemaVersion(message))
                        .build();
            }
            throw new IllegalStateException("The provided content type is not supported");
        }

        private void publish(HermesMessage message) {
            hermesClient.publish(message).whenComplete((resp, exc) -> {
                if (resp.isSuccess()) {
                    logger.debug("Message published successfully to Hermes");
                } else {
                    logError(resp);
                }
            });
        }

        private int getSchemaVersion(Message<?> message) {
            final Optional<String> schemaVersion = getHeader(message, SCHEMA_VERSION_HEADER);
            if (schemaVersion.isPresent()) {
                return Integer.parseInt(schemaVersion.get());
            }
            throw new IllegalStateException("Header 'Schema-Version' is required for AVRO message");
        }

        private Optional<String> getContentType(Message<?> message) {
            return getHeader(message, MessageHeaders.CONTENT_TYPE);
        }

        private Optional<String> getHeader(Message<?> message, String headerName) {
            final Object contentType = message.getHeaders().get(headerName);
            if (contentType != null) {
                return Optional.of(String.valueOf(contentType));
            }
            return Optional.empty();
        }

        private byte[] getPayloadAsBytes(Message<?> message) {
            if (message.getPayload() instanceof byte[]) {
                return (byte[]) message.getPayload();
            } else if (message.getPayload() instanceof String) {
                return ((String) message.getPayload()).getBytes(UTF_8);
            }
            return null;
        }

        private void logError(HermesResponse resp) {
            if (resp.getFailureCause().isPresent()) {
                logger.error("Failed to publish message to Hermes endpoint", resp.getFailureCause().get());
            } else {
                logger.error("Unknown error has occurred when publish message to Hermes endpoint");
            }
        }
    }
}
