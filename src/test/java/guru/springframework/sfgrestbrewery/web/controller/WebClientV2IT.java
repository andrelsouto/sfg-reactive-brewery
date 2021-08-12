package guru.springframework.sfgrestbrewery.web.controller;

import guru.springframework.sfgrestbrewery.bootstrap.BeerLoader;
import guru.springframework.sfgrestbrewery.web.functional.BeerRouterConfig;
import guru.springframework.sfgrestbrewery.web.model.BeerDto;
import guru.springframework.sfgrestbrewery.web.model.BeerPagedList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.math.BigDecimal;
import java.sql.Time;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class WebClientV2IT {

    public static final String BASE_URL = "http://localhost:8080";

    WebClient webClient;

    @BeforeEach
    void setUp() {
        webClient = WebClient.builder()
                .baseUrl(BASE_URL)
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create().wiretap(true)))
                .build();
    }

    @Test
    void testeDeleteBeer() {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        webClient.delete().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(3))
                .retrieve().toBodilessEntity()
                .flatMap(resposenEntity -> {
                    countDownLatch.countDown();

                    return webClient.get().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(3))
                            .accept(MediaType.APPLICATION_JSON)
                            .retrieve().bodyToMono(BeerDto.class);
                }).subscribe(savedBeer -> {

                }, throwable -> countDownLatch.countDown());

    }

    @Test
    void testDeleteBeerNotFound() {

        webClient.delete().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(4))
                .retrieve().toBodilessEntity().block();

        assertThrows(WebClientResponseException.NotFound.class, () -> webClient.delete().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(4))
                .retrieve().toBodilessEntity().block());
    }

    @Test
    void testUpdateBeer() throws InterruptedException {

        final String newBeerName = "ALs Beer";
        final Integer beerId = 1;
        CountDownLatch countDownLatch = new CountDownLatch(2);

        webClient.put().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(beerId))
                .accept(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(BeerDto.builder().beerName(newBeerName)
                        .upc("1233455")
                        .beerStyle("PALE_ALE")
                        .price(new BigDecimal("8.99")).build()))
                .retrieve().toBodilessEntity()
                .subscribe(responseEntity -> {
                    assertThat(responseEntity.getStatusCode().is2xxSuccessful());
                    countDownLatch.countDown();
                });

        countDownLatch.await(500, TimeUnit.MILLISECONDS);

        webClient.get().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(beerId))
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class)
                .subscribe(beerDto -> {
                    assertThat(beerDto).isNotNull();
                    assertThat(beerDto.getBeerName()).isNotNull();
                    assertThat(beerDto.getBeerName()).isEqualTo(newBeerName);
                    countDownLatch.countDown();
                });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testUpdateBeerNotFound() throws InterruptedException {

        final String newBeerName = "ALs Beer";
        final Integer beerId = 999;
        CountDownLatch countDownLatch = new CountDownLatch(1);

        webClient.put().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_ID).build(beerId))
                .accept(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(BeerDto.builder().beerName(newBeerName)
                        .upc("1233455")
                        .beerStyle("PALE_ALE")
                        .price(new BigDecimal("8.99")).build()))
                .retrieve().toBodilessEntity()
                .subscribe(responseEntity -> {
                }, throwable -> {
                    countDownLatch.countDown();
                });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testSaveBeer() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        BeerDto beerDto = BeerDto.builder()
                .beerName("JTs Beer")
                .upc("1233455")
                .beerStyle("PALE_ALE")
                .price(new BigDecimal("8.99"))
                .build();

        Mono<ResponseEntity<Void>> responseEntityMono = webClient.post().uri(BeerRouterConfig.BEER_V2_URL)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(beerDto))
                .retrieve().toBodilessEntity();

        responseEntityMono.publishOn(Schedulers.parallel()).subscribe(responseEntity -> {
            assertThat(responseEntity.getStatusCode().is2xxSuccessful());

            countDownLatch.countDown();
        });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void testBeerBadRequest() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        BeerDto beerDto = BeerDto.builder()
                .price(new BigDecimal("8.99"))
                .build();

        Mono<ResponseEntity<Void>> responseEntityMono = webClient.post().uri(BeerRouterConfig.BEER_V2_URL)
                .accept(MediaType.APPLICATION_JSON).body(BodyInserters.fromValue(beerDto))
                .retrieve().toBodilessEntity();

        responseEntityMono.publishOn(Schedulers.parallel()).subscribe(responseEntity -> {
        }, throwable -> {
            if (throwable.getClass().getName().equals("org.springframework.web.reactive.function.client.WebClientResponseException$BadRequest")) {
                WebClientResponseException ex = (WebClientResponseException) throwable;

                if (ex.getStatusCode().equals(HttpStatus.BAD_REQUEST)){
                    countDownLatch.countDown();
                }
            }
        });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerById() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BeerRouterConfig.BEER_V2_URL + "/1")
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {
            assertThat(beer).isNotNull();
            assertThat(beer.getBeerName()).isNotNull();

            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByIdNotFound() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(BeerRouterConfig.BEER_V2_URL + "/13333")
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {

        }, throwable -> countDownLatch.countDown());

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByUpc() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_UPC).build(BeerLoader.BEER_2_UPC))
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {
            assertThat(beer).isNotNull();
            assertThat(beer.getBeerName()).isNotNull();

            countDownLatch.countDown();
        });

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }

    @Test
    void getBeerByUpcNotFound() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono<BeerDto> beerDtoMono = webClient.get().uri(uriBuilder -> uriBuilder.path(BeerRouterConfig.BEER_V2_URL_UPC).build("1232141"))
                .accept(MediaType.APPLICATION_JSON)
                .retrieve().bodyToMono(BeerDto.class);

        beerDtoMono.subscribe(beer -> {
        }, throwable -> countDownLatch.countDown());

        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
        assertThat(countDownLatch.getCount()).isEqualTo(0);
    }
}
