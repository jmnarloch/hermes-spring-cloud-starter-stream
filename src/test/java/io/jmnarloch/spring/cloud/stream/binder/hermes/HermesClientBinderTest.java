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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import pl.allegro.tech.hermes.client.HermesClientBuilder;
import pl.allegro.tech.hermes.client.HermesMessage;
import pl.allegro.tech.hermes.client.HermesResponse;
import pl.allegro.tech.hermes.client.HermesResponseBuilder;
import pl.allegro.tech.hermes.client.HermesSender;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link HermesClientBinder} class.
 *
 * @author Jakub Narloch
 */
public class HermesClientBinderTest {

    private static final String OUTPUT_NAME = "topic";

    private static final String MESSAGE = "Hello";

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private HermesSender hermesSender;

    private HermesClientBinder binder;

    @Before
    public void setUp() throws Exception {

        final HermesResponse response = HermesResponseBuilder.hermesResponse()
                .withHttpStatus(201)
                .build();

        when(hermesSender.send(any(URI.class), any(HermesMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        binder = new HermesClientBinder(HermesClientBuilder.hermesClient(hermesSender).build());
        binder.setApplicationContext(new GenericApplicationContext());
    }

    @Test
    public void shouldPublishMessage() {

        // given
        DirectChannel output = new DirectChannel();

        // when
        Binding<MessageChannel> binding = binder.bindProducer(
                OUTPUT_NAME, output, new ExtendedProducerProperties<>(new HermesProducerProperties()));

        // then
        output.send(new GenericMessage<>(MESSAGE, json()));
        verify(hermesSender).send(any(URI.class), any(HermesMessage.class));
        binding.unbind();
    }

    @Test
    public void shouldPublishMessageWithBytePayload() {

        // given
        DirectChannel output = new DirectChannel();

        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        ArgumentCaptor<HermesMessage> messageCaptor = ArgumentCaptor.forClass(HermesMessage.class);

        // when
        Binding<MessageChannel> binding = binder.bindProducer(
                OUTPUT_NAME, output, new ExtendedProducerProperties<>(new HermesProducerProperties()));

        // then
        output.send(new GenericMessage<>(MESSAGE, json()));
        verify(hermesSender).send(uriCaptor.capture(), messageCaptor.capture());

        assertEquals("http://localhost:8080/topics/topic", uriCaptor.getValue().toString());
        assertArrayEquals(MESSAGE.getBytes(), messageCaptor.getValue().getBody());

        binding.unbind();
    }

    @Test
    public void shouldPublishMessageWithError() {

        // given
        reset(hermesSender);
        final HermesResponse response = HermesResponseBuilder.hermesResponse()
                .withHttpStatus(500)
                .build();

        when(hermesSender.send(any(URI.class), any(HermesMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        DirectChannel output = new DirectChannel();

        // when
        Binding<MessageChannel> binding = binder.bindProducer(
                OUTPUT_NAME, output, new ExtendedProducerProperties<>(new HermesProducerProperties()));

        // then
        output.send(new GenericMessage<>(MESSAGE, json()));
        verify(hermesSender, times(4)).send(any(URI.class), any(HermesMessage.class));
        binding.unbind();
    }

    private static Map<String, Object> json() {
        return Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    }
}