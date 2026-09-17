# Memoria del proyecto — build-your-kafka

Contexto persistente para retomar el proyecto en cualquier sesión futura (humana o de Claude Code). Última actualización: 2026-09-14.

## Qué es esto

Clon simplificado de Apache Kafka en Java, siguiendo la serie de artículos "Build Your Own Kafka" (buildthingsuseful, Medium). Empezó como proyecto personal de aprendizaje y ahora también es la entrega de la materia **Sistemas Distribuidos** — individual, metodología **Kanban** exigida por la cátedra.

Repo: `github.com/Chillipeeper1/build-your-kafka` — package base `com.simplekafka.broker`.

## Fuentes del artículo guía

| Etapa | Tema | Link |
|---|---|---|
| 3 | Cliente de ZooKeeper | https://buildthingsuseful.medium.com/stage-3-build-your-own-kafka-3baaf4c873de |
| 4 | Capa de almacenamiento | https://buildthingsuseful.medium.com/stage-4-build-your-own-kafka-building-the-storage-layer-7a8497a614f3 |
| 5 | El broker | https://buildthingsuseful.medium.com/stage-5-build-your-own-kafka-building-the-broker-e516c4757356 |
| 6 | Librería cliente | https://buildthingsuseful.medium.com/stage-6-build-your-own-kafka-developing-the-client-library-96ecc0218089 |
| 7 | APIs de producer/consumer | https://buildthingsuseful.medium.com/stage-7-build-your-own-kafka-building-higher-level-producer-and-consumer-apis-2fba12ecf062 |
| 8 | Pruebas del sistema | https://buildthingsuseful.medium.com/stage-8-build-your-own-kafka-test-the-system-39ca1c1dca5e |

Medium bloquea el fetch directo (paywall/bot-blocking); funciona pasando la URL por `https://r.jina.ai/<url>`.

## Estado actual: capa de control (Etapa 3) — completa y verificada

**`ZooKeeperClient`**
- `connect()` — conexión asíncrona con patrón `Watcher` + `CountDownLatch`, crea `/brokers`, `/brokers/ids`, `/topics` como nodos persistentes.
- `createPersistentNode(path, data)` / `createEphemeralNode(path, data)` — creación genérica; la efímera devuelve `boolean` (si ganaste la carrera por crearla).
- `watchChildren(path, callback)` / `watchNode(path, callback)` — watches reactivos con re-armado recursivo (un watch de ZooKeeper es de un solo disparo).
- `process(WatchedEvent)` — maneja `SyncConnected` (destraba el latch), `Disconnected` (no-op, la librería reconecta sola), `Expired` (recrea el objeto `ZooKeeper` + el latch).

**`Broker`**
- `start()` → `connect()` + `registerBroker()` + `electController()`.
- `registerBroker()` — nodo efímero en `/brokers/ids/<id>` con `host:puerto`.
- `electController()` — compite por `/controller` (efímero); el que pierde queda con `watchNode` esperando la vacante.

**`BrokerInfo`** — record simple (id, host, puerto). **`Protocol`** — vacío, es lo próximo.

Todo esto se probó en vivo contra un ZooKeeper real en Docker (`docker run -d --name zookeeper -p 2181:2181 zookeeper`), incluyendo una carrera real de 2 brokers por la elección de controller.

## Bugs de integración ya resueltos (no repetirlos)

1. `connect()` NO debe crear `/controller` como nodo persistente — rompe la elección, porque `createEphemeralNode` ve que ya existe y nadie gana nunca. `/controller` es el premio efímero en sí mismo, no una carpeta.
2. `/brokers/ids` sí necesita crearse como padre persistente en `connect()` — ZooKeeper no crea un hijo si el padre no existe (no es como `mkdir -p`).
3. Forzar un `Expired` real de sesión con `docker restart`/`stop`+`start` **no funcionó** ni con 8s de caída — el server persiste la sesión en su log de transacciones y el cliente reconecta directo (`Disconnected` → `SyncConnected`). Para verlo de verdad haría falta la técnica de "session hijack" (abrir un segundo cliente con el mismo sessionId+password y cerrarlo).

## Qué falta (plano de datos — nada de esto existe todavía)

- Etapa 4: `Partition` — log segmentado en disco (chunks de 1MB) + archivos de índice offset→posición, `ReadWriteLock` para lectura/escritura concurrente, replicación líder-seguidor a nivel partición.
- Etapa 5: `SimpleKafkaBroker` — servidor de red, produce/fetch/replicate. `Broker` todavía no escribe metadata de topics bajo `/topics` (el método existe, nadie lo llama para eso).
- Etapa 6: `SimpleKafkaClient` — descubrimiento de brokers + enrutamiento por partición.
- Etapa 7: `SimpleKafkaProducer` / `SimpleKafkaConsumer` sobre el cliente.
- Etapa 8: pruebas multi-broker con tolerancia a fallos real.

Hoy, mandar un mensaje es un no-op literal: no hay servidor de red, ni storage, ni nadie escribiendo metadata de topics.

## Cómo se trabaja en este proyecto (estilo de colaboración)

El dueño del proyecto está **aprendiendo** sistemas distribuidos construyendo, no shippeando — escribe el código de cada clase él mismo; Claude revisa, compila (`mvn compile`) y señala bugs con la causa raíz explicada, sin escribir la lección por él. Arneses de prueba descartables (`Main.java`, `println` temporales) sí los escribe Claude directamente.

Verificación en vivo por sobre confiar solo en la lógica: contenedor de ZooKeeper local corriendo, classpath con `mvn dependency:build-classpath -Dmdep.outputFile=cp.txt`, correr con `java -cp "target/classes;$(cat cp.txt)" com.simplekafka.broker.<Clase>`. `zkCli.sh` dentro del contenedor (`docker exec zookeeper bash -c "echo '<cmd>' | zkCli.sh -server localhost:2181"`) sirve para inspeccionar/mutar znodes a mano y simular otros miembros del cluster.

Cuando hay una decisión de arquitectura real (no solo una corrección), se le presentan las opciones al dueño del proyecto y decide él.

## Documento de propuesta académica

Ya generado: `propuesta-proyecto.pdf` en la raíz del repo (Objetivo, Justificación, Análisis de requisitos con 7 RF confirmados contra las fuentes, Metodología y roles — Kanban individual). No está commiteado al repo todavía, es decisión del dueño si lo versiona ahí o lo entrega directo por el portal de la materia.
