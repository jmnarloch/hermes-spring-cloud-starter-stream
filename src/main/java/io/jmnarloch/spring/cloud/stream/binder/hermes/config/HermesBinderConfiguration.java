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
package io.jmnarloch.spring.cloud.stream.binder.hermes.config;

import io.jmnarloch.spring.cloud.stream.binder.hermes.HermesClientBinder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.AsyncRestTemplate;
import pl.allegro.tech.hermes.client.HermesClient;
import pl.allegro.tech.hermes.client.HermesClientBuilder;
import pl.allegro.tech.hermes.client.HermesSender;
import pl.allegro.tech.hermes.client.restTemplate.RestTemplateHermesSender;

import javax.xml.bind.Binder;

/**
 * Configures the Hermes binder.
 *
 * @author Jakub Narloch
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties(HermesBinderProperties.class)
public class HermesBinderConfiguration {

    @Autowired
    private HermesBinderProperties hermesBinderProperties;

    @Bean
    @ConditionalOnMissingBean
    public HermesClientBinder hermesClientBinder(HermesClient hermesClient) {
        return new HermesClientBinder(hermesClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public HermesClient hermesClient(HermesSender hermesSender) {
        return HermesClientBuilder.hermesClient(hermesSender)
                .withURI(hermesBinderProperties.getUri())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    public HermesSender restTemplateHermesSender(AsyncRestTemplate restTemplate) {
        return new RestTemplateHermesSender(restTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncRestTemplate asyncRestTemplate() {
        return new AsyncRestTemplate();
    }
}
