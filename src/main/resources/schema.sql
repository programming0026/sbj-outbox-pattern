-- Negocio
create table if not exists orders (
    id          uuid primary key,
    customer    varchar(100)   not null,
    amount      numeric(12, 2) not null,
    status      varchar(20)    not null,
    created_at  timestamptz    not null
);

-- OUTBOX: eventos a publicar. Se escribe en la misma transacción que el cambio de negocio.
create table if not exists outbox_events (
    id             uuid primary key,
    aggregate_type varchar(50)  not null,
    aggregate_id   varchar(64)  not null,
    event_type     varchar(50)  not null,
    payload        text         not null,
    created_at     timestamptz  not null,
    published_at   timestamptz,           -- NULL = pendiente de publicar
    attempts       int          not null default 0,
    last_error     text
);

-- Índice parcial: solo contiene los pendientes, así que el relay los encuentra rápido aunque la tabla tenga millones de filas
create index if not exists idx_outbox_pending on outbox_events (created_at, id) where published_at is null;

-- Lado consumidor
create table if not exists notifications (
    id           bigserial primary key,
    event_id     uuid         not null unique,
    event_type   varchar(50)  not null,
    aggregate_id varchar(64)  not null,
    payload      text         not null,
    received_at  timestamptz  not null
);
create index if not exists idx_notifications_aggregate on notifications (aggregate_id);

-- INBOX: eventos ya procesados (para descartar duplicados)
create table if not exists processed_messages (
    event_id     uuid primary key,
    processed_at timestamptz not null
);
