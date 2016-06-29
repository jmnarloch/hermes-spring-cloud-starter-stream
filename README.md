# Spring Cloud Stream Hermes binder

> A Sprning Cloud Stream binder for Allegro Hermes

[![Build Status](https://travis-ci.org/jmnarloch/hermes-spring-cloud-starter-stream.svg?branch=master)](https://travis-ci.org/jmnarloch/hermes-spring-cloud-starter-stream)
[![Coverage Status](https://coveralls.io/repos/jmnarloch/hermes-spring-cloud-starter-stream/badge.svg?branch=master&service=github)](https://coveralls.io/github/jmnarloch/hermes-spring-cloud-starter-stream?branch=master)

## Setup

Add the Spring Cloud starter to your project:

```xml
<dependency>
  <groupId>io.jmnarloch</groupId>
  <artifactId>hermes-spring-cloud-starter-stream</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Features

This project adds a binder for [Allegro Hermes](https://github.com/allegro/hermes) to [Spring Cloud Stream](https://github.com/spring-cloud/spring-cloud-stream).

By the way the Hermes has been designed there is simple way to create a consumer so at this point only binding a
producer `MessageChannel` is being supported.

### Creating the binding

Creating the bindings for the Spring Cloud Stream is straight forward and

You can always bind to the generic `Source` using `@EnableBinding` and afterwards specify the final destination
using the properties like so:

```
spring:
  cloud:
    stream:
      bindings:
        output:
          destination: 'io.jmnarloch.events.purchases'
```

In this way you specify the fully qualified name of the Hermes topic.

Alternative approach is to define the your own channel binding, example:

```
interface Events {

        @Output
        MessageChannel purchases();
}
```

In this case the topic name will be establish based on the method name, if you want to use specific topic name you can
do this by specific the value of the `@Output`. Finally you can specify the destination property in your configuration
as mentioned previously.

### Binder properties

`spring.cloud.stream.hermes.binder.uri` - specifies the Hermes producer URI

## License

Apache 2.0