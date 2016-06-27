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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.messaging.MessageHeaders.CONTENT_TYPE;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

/**
 * Demonstrates the usage of this component.
 *
 * @author Jakub Narloch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Demo.Application.class)
@WebAppConfiguration
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class Demo {

    @Autowired
    private Events events;

    @Rule
    public final WireMockRule wireMock = new WireMockRule(8765);

    @Before
    public void setUp() throws Exception {

        wireMock.stubFor(post(urlPathMatching("/topics/purchases"))
                .willReturn(aResponse().withStatus(201)));
    }

    @Test
    public void shouldInitializeHermesSource() {

        assertNotNull(events);
    }

    @Test
    public void shouldPublishTextHermesMessage() {

        // given
        final Message<String> message = MessageBuilder.withPayload("Hello Hermes!")
                .setHeaderIfAbsent(CONTENT_TYPE, APPLICATION_JSON_VALUE)
                .build();

        // when
        events.purchases().send(message);

        // then
        await().atMost(1, TimeUnit.SECONDS);
        verify(1, postRequestedFor(urlPathMatching("/topics/purchases")));
    }

    @Test
    public void shouldPublishByteHermesMessage() {

        // given
        final Message<byte[]> message = MessageBuilder.withPayload("Hello Hermes!".getBytes(UTF_8))
                .setHeaderIfAbsent(CONTENT_TYPE, APPLICATION_JSON_VALUE)
                .build();

        // when
        events.purchases().send(message);

        // then
        await().atMost(1, TimeUnit.SECONDS);
        verify(1, postRequestedFor(urlEqualTo("/topics/purchases")));
    }

    interface Events {

        @Output
        MessageChannel purchases();
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableBinding(Events.class)
    public static class Application {

    }
}
