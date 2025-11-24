# Middleware de mensajes

Se implementa un paquete dentro del modulo de `common` siguiendo [la interfaz delimitada por la catedra](https://github.com/7574-sistemas-distribuidos/middleware-interfaces/blob/main/Golang/middleware.go).

Este paquete contiene dos _'flavors'_ del middleware, uno utilizando solamente colas de [RabbitMQ](rabbitmq.com) y otro usando exchanges y colas asociadas al mismo.

A su vez se valida el funcionamiento de cada uno bajo los siguientes escenarios con pruebas de integracion que requieren de una instancia corriendo de rabbitMQ en `test/integration/`.
- 1 sender 1 receiver.
- 1 sender N receivers.
- N senders 1 receiver.

## Orientado a _working queues_

Para el uso del mismo se crea un middleware con un nombre de cola y luego se pueden tomar los roles de:
- Producer: Utiliza el metodo `Send` enviando una tira de bytes.
- Consumer: Utiliza el metodo `StartConsuming` proporcionando un callback que leera del canal de mensajes.

ejemplo de uso:
```go
// -- As a consumer: --
m, _ := middleware.NewQueueMiddleware(url, "myqueue")
defer receiver.StopConsuming()
defer receiver.Close()
// defer receiver.Delete() if delete is needed

done := make(chan bool, 1)

// This call is -- Non Blocking --
_ := receiver.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
    messagesReceived := 0
    for msg := range consumeChannel {
        // productive work ...
    }
    done <- true
})

// If synchronization is needed (e.g. with timeout)
select {
case <-done:
case <-time.After(2 * time.Second):
    logger.Logger.Fatal("did not receive message in time")
}

// -- As a sender: --
m, _ := middleware.NewQueueMiddleware(url, "myqueue")

_ = sender.Send([]byte("Hello World!"))
```

## Orientado a _exchanges_

Para el uso del mismo se crea un middleware con un nombre y tipo de exchange, routing keys y luego se pueden tomar los roles de:
- Publisher: Utiliza el metodo `Send` enviando una tira de bytes (y se utilizan las routing keys provistas).
- Subscriber: Utiliza el metodo `StartConsuming` proporcionando un callback que leera del canal de mensajes (recibiendo de las routing keys provistas).

ejemplo de uso:
```go
// -- As a subscriber: --
m, _ := middleware.NewExchangeMiddleware(url, "myexchange", "direct", []string{"key1", "key2"})
defer receiver.StopConsuming()
defer receiver.Close()
// defer receiver.Delete() if delete is needed

done := make(chan bool, 1)

// This call is -- Non Blocking --
_ := receiver.StartConsuming(func(consumeChannel middleware.ConsumeChannel, d chan error) {
    messagesReceived := 0
    for msg := range consumeChannel {
        // productive work with message comming from any of the provided keys ...
    }
    done <- true
})

// If synchronization is needed (e.g. with timeout)
select {
case <-done:
case <-time.After(2 * time.Second):
    logger.Logger.Fatal("did not receive message in time")
}

// -- As a publisher: --
m, _ := middleware.NewExchangeMiddleware(url, "myexchange", "direct", []string{"key1", "key2", "key3"})

_ = sender.Send([]byte("Hello World!")) // Sends to all provided routing keys
```

Tipos de exchange disponibles:
- fanout: ignora las route keys y envia a todas las colas asociadas
- direct: envia a las colas asociadas con la route key exacta
- topic: envia a las colas asociadas con un route key match (usando wildcards)

## How to test

Para correr los tests de integracion provistos se necesita tener rabbitMQ corriendo y luego ejecutar los tests:

```bash
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management &

go test ./src/tests/integration/... # agregar -count=1 si se quiere correr sin cache
```
