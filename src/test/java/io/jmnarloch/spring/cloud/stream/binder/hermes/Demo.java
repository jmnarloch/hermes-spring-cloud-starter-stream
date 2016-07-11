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
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.given;
import static org.junit.Assert.assertNotNull;
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

    private static final String PURCHASES_TOPIC_PATH = "/topics/pl.allegro.payment.purchases";

    private static final String RETURNS_TOPIC_PATH = "/topics/pl.allegro.payment.returns";

    @Autowired
    private Events events;

    @Rule
    public final WireMockRule wireMock = new WireMockRule(8765);

    @Before
    public void setUp() throws Exception {

        stubTopicEndpoint(PURCHASES_TOPIC_PATH, MediaType.APPLICATION_JSON_VALUE);
        stubTopicEndpoint(RETURNS_TOPIC_PATH, "avro/binary");
    }

    @Test
    public void shouldInitializeHermesSource() {

        assertNotNull(events);
    }

    @Test
    public void shouldPublishPojoHermesMessage() {

        // given
        final UUID id = UUID.randomUUID();

        // and
        final Message<Purchase> message = MessageBuilder
                .withPayload(new Purchase(id))
                .build();

        // when
        events.purchases().send(message);

        // then
        given().ignoreExceptions().await().atMost(5, SECONDS).until(
                () -> wireMock.verify(
                        1,
                        postRequestedFor(urlPathMatching("/topics/pl.allegro.payment.purchases"))
                                .withRequestBody(equalToJson(String.format("{\"id\": \"%s\"}", id.toString())))
                )
        );
    }

    @Test
    public void shouldPublishTextHermesMessage() {

        // given
        final Message<String> message = MessageBuilder
                .withPayload("Hello Hermes!")
                .build();

        // when
        events.purchases().send(message);

        // then
        given().ignoreExceptions().await().atMost(5, SECONDS).until(
                () -> wireMock.verify(1, postRequestedFor(urlEqualTo(PURCHASES_TOPIC_PATH)))
        );
    }

    @Test
    public void shouldPublishByteHermesMessage() {

        // given
        final Message<byte[]> message = MessageBuilder
                .withPayload("Hello Hermes!".getBytes(UTF_8))
                .build();

        // when
        events.purchases().send(message);

        // then
        given().ignoreExceptions().await().atMost(5, SECONDS).until(
                () -> wireMock.verify(1, postRequestedFor(urlEqualTo(PURCHASES_TOPIC_PATH)))
        );
    }

    @Test
    public void shouldPublishAvroMessage() {

        // given
        final Message<byte[]> message = MessageBuilder
                .withPayload(new byte[0])
                .setHeader(MessageHeaders.CONTENT_TYPE, "avro/binary")
                .setHeader("Schema-Version", 1)
                .build();

        // when
        events.returns().send(message);

        // then
        given().ignoreExceptions().await().atMost(5, SECONDS).until(
                () -> wireMock.verify(1,
                        postRequestedFor(urlEqualTo(RETURNS_TOPIC_PATH))
                                .withHeader("Content-Type", equalTo("avro/binary"))
                                .withHeader("Schema-Version", equalTo("1"))
                )
        );
    }

    private void stubTopicEndpoint(String path, String contentType) {
        wireMock.stubFor(post(urlPathMatching(path))
                .withHeader(HttpHeaders.CONTENT_TYPE, equalTo(contentType))
                .willReturn(aResponse().withStatus(201)));
    }

    interface Events {

        @Output
        MessageChannel purchases();

        @Output
        MessageChannel returns();
    }

    public static class Purchase {

        private final UUID id;

        public Purchase(UUID id) {
            this.id = id;
        }

        public UUID getId() {
            return id;
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @EnableBinding(Events.class)
    public static class Application {

    }
}
