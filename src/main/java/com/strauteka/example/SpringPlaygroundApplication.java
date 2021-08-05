package com.strauteka.example;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
@Component
@EnableScheduling
@SpringBootApplication
public class SpringPlaygroundApplication {

    private final WebClient webClient;

    public SpringPlaygroundApplication(
            @Lazy WebClient webClient) {
        this.webClient = webClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringPlaygroundApplication.class, args);
    }

    @GetMapping(value = "ping/buffered/{times}/{delay}",
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_NDJSON_VALUE})
    Flux<List<Pong>> pongBuf(@PathVariable("times") Integer times,
                             @PathVariable("delay") Integer delay) {
        return Flux
                .generate(() -> 1L, (state, sink) -> {
                    sink.next(state);
                    return state + 1L;
                })
                .take(times)
                .delayElements(Duration.ofMillis(delay))
                .map(n -> {
                    log.warn("creating Pong " + n);
                    return new Pong("Pong" + n);
                })
                .buffer(2).doOnNext(n -> log.info("Sending {}", n.size()))
                .doFinally(sign -> log.info("End {}", sign));
    }

    // Could not successfully cast to List<List<Pong>> or List<Pong>.
    // As from logs, in Encoding phase, comes in all data
//    Encoding [[[SpringPlaygroundApplication.Pong(pong=Pong1), SpringPlaygroundApplication.Pong(pong=Pong2)], [Spri (truncated)...]
    //So maybe you can't cast use bodyToFlux and it should be bodyToMono?
    @Scheduled(fixedRate = 20000)
    public void ping() {
        pingDefault()
                .bodyToFlux(new ParameterizedTypeReference<String>(){})
//                .bodyToFlux(new ParameterizedTypeReference<List<Pong>>(){})
                .doOnNext(item -> log.info("Flux Next {}", item))
                .onErrorContinue((a, b) -> log.error("{}/{}", a, b))
                .subscribe(n -> log.info("Flux result: {}", n));
    }


    @Scheduled(fixedRate = 20000, initialDelay = 10000)
    public void pingMonoBlock() {
        pingDefault()
                .bodyToMono(new ParameterizedTypeReference<List<List<Pong>>>(){})
                .doOnNext(e -> log.info("Mono Next {}", e))
                .onErrorContinue((a, b) -> log.error("{}/{}", a, b))
                .subscribe(n -> log.info("Mono result: {}", n));
    }

    private WebClient.ResponseSpec pingDefault() {
        return webClient.get().uri(uriBuilder -> uriBuilder
                .path("/ping/buffered/5/200")
                .build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve();
    }

    @SneakyThrows
    @Bean
    public WebClient webClient() {
        int port = 8080;
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .responseTimeout(Duration.ofMillis(5000))
                .doOnConnected(conn ->
                        conn.addHandlerLast(new ReadTimeoutHandler(5000, TimeUnit.MILLISECONDS))
                                .addHandlerLast(new WriteTimeoutHandler(5000, TimeUnit.MILLISECONDS)))
                .host(InetAddress.getLocalHost().getHostAddress())
                .port(port);

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.ACCEPT,
                        MediaType.APPLICATION_NDJSON_VALUE,
                        MediaType.APPLICATION_JSON_VALUE)
                //.exchangeStrategies(strategies)
                .build();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Pong {
        String pong;
    }
}
