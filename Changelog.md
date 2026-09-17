# Changelog

Todas las entradas están en orden cronológico inverso (más reciente arriba). Formato libre — proyecto de aprendizaje, no versionado por releases.

## 2026-09-14

### Added
- Documento de propuesta académica (`propuesta-proyecto.pdf`) para la materia Sistemas Distribuidos: Objetivo, Justificación, Análisis de requisitos (7 requisitos funcionales, confirmados contra las 6 fuentes del artículo guía), Metodología y roles (Kanban, individual).
- `memory.md` y este `Changelog.md`.

### Changed
- Confirmado el roadmap completo del proyecto contra las etapas 3 a 8 del artículo guía (antes solo se conocía la etapa 3).

## 2026-09-13

### Added
- `Broker.java`: `start()`, `registerBroker()` (nodo efímero en `/brokers/ids/<id>`), `electController()` (elección vía `createEphemeralNode("/controller", ...)`, con `watchNode` para el que pierde).
- `BrokerInfo.java`: record de identidad del broker (id, host, puerto).
- Repositorio git independiente para el proyecto (antes vivía sin trackear dentro de un repo git accidental en el home del usuario) y publicado en `github.com/Chillipeeper1/build-your-kafka`.

### Fixed
- `connect()` ya no crea `/controller` como nodo persistente — rompía la elección de controller, porque `createEphemeralNode` siempre veía el nodo como "ya existente" y nadie ganaba nunca.
- `connect()` ahora crea `/brokers/ids` como nodo persistente padre (ZooKeeper no permite crear un hijo si el padre no existe).

### Verified (en vivo contra ZooKeeper real en Docker)
- `connect()` + creación idempotente de `/brokers`, `/brokers/ids`, `/topics`.
- `watchChildren` reaccionando en tiempo real a un nodo creado externamente (vía `zkCli.sh`).
- `process()` reconectando ante `Disconnected` sin intervención.
- Elección de controller entre 2 brokers reales corriendo en paralelo — un ganador, un perdedor vigilando la vacante.
- (Intentado, no logrado) forzar un evento `Expired` real vía `docker restart`/`stop`+`start` — el server ZooKeeper persiste la sesión en su log de transacciones y el cliente reconecta directo sin expirar, incluso con 8s de caída.

## 2026-09-12

### Fixed
- `pom.xml` inválido (URLs de namespace con `<...>` literales) que impedía compilar el proyecto entero.

### Added
- `ZooKeeperClient.java` completo, construido método por método:
  - Campos (`connectString` final, `zooKeeper`/`connectedSignal` mutables, `SESSION_TIMEOUT` constante).
  - `connect()` con patrón conexión asíncrona + `Watcher` + `CountDownLatch`.
  - `createPersistentNode(path, data)` / `createEphemeralNode(path, data)`.
  - `watchChildren(path, callback)` / `watchNode(path, callback)` con re-armado recursivo del watch (de un solo disparo en ZooKeeper).
  - `process(WatchedEvent)` completo: `SyncConnected`, `Disconnected` (no-op intencional), `Expired` (recrea conexión + latch).
