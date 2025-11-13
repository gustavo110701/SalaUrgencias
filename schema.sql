-- ================================
-- Ajustes recomendados para SQLite
-- ================================
PRAGMA foreign_keys = ON;        -- Habilitar llaves foráneas
PRAGMA journal_mode = WAL;       -- Mejor concurrencia
PRAGMA synchronous = NORMAL;     -- Buen balance entre seguridad y rendimiento

-- ===========================================
-- 1) SALAS (identidad de cada nodo)
-- ===========================================
CREATE TABLE IF NOT EXISTS SALAS (
  id_sala     INTEGER PRIMARY KEY,        -- 1..N
  nombre      TEXT NOT NULL,
  ip_sala     TEXT NOT NULL,
  es_maestro  INTEGER NOT NULL DEFAULT 0, -- 1=maestro, 0=réplica
  activa      INTEGER NOT NULL DEFAULT 1  -- 1=operativa, 0=caída
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_salas_ip ON SALAS(ip_sala);

-- ==============================================
-- 2) DOCTORES
-- ==============================================
CREATE TABLE IF NOT EXISTS DOCTORES (
  id_doctor     INTEGER PRIMARY KEY,
  nombre        TEXT NOT NULL,
  especialidad  TEXT,
  id_sala_base  INTEGER,
  activo        INTEGER NOT NULL DEFAULT 1,
  disponible    INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (id_sala_base) REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_doctores_disponible ON DOCTORES(disponible, activo);

-- =============================================
-- 3) PACIENTES
-- =============================================
CREATE TABLE IF NOT EXISTS PACIENTES (
  id_paciente          INTEGER PRIMARY KEY,
  nombre               TEXT NOT NULL,
  edad                 INTEGER,
  sexo                 TEXT,
  curp                 TEXT UNIQUE,
  telefono             TEXT,
  contacto_emergencia  TEXT,
  activo               INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS ix_pacientes_activo ON PACIENTES(activo);

-- =====================================================
-- 4) TRABAJADORES_SOCIALES
-- =====================================================
CREATE TABLE IF NOT EXISTS TRABAJADORES_SOCIALES (
  id_trabajador  INTEGER PRIMARY KEY,
  nombre         TEXT NOT NULL,
  id_sala        INTEGER,
  activo         INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (id_sala) REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

-- ===========================================
-- 5) CAMAS
-- ===========================================
CREATE TABLE IF NOT EXISTS CAMAS (
  id_cama     INTEGER NOT NULL,
  id_sala     INTEGER NOT NULL,
  descripcion TEXT,
  estado      TEXT NOT NULL DEFAULT 'LIBRE', -- LIBRE | OCUPADA | FUERA_SERVICIO
  PRIMARY KEY (id_cama, id_sala),
  FOREIGN KEY (id_sala) REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_camas_estado ON CAMAS(estado, id_sala);

-- =====================================================
-- 6) VISITAS_EMERGENCIA
-- =====================================================
CREATE TABLE IF NOT EXISTS VISITAS_EMERGENCIA (
  id_visita         INTEGER PRIMARY KEY AUTOINCREMENT,
  folio             TEXT UNIQUE,
  id_paciente       INTEGER NOT NULL,
  id_doctor         INTEGER NOT NULL,
  id_trabajador     INTEGER NOT NULL,
  id_sala           INTEGER NOT NULL,          -- sala que atiende
  id_cama           INTEGER NOT NULL,
  origen_solicitud  INTEGER,
  prioridad         INTEGER NOT NULL DEFAULT 3,
  motivo            TEXT,
  fecha_hora_inicio TEXT NOT NULL,
  fecha_hora_cierre TEXT,
  estado            TEXT NOT NULL DEFAULT 'ABIERTA',  -- ABIERTA | CERRADA | REASIGNADA

  FOREIGN KEY (id_paciente)   REFERENCES PACIENTES(id_paciente)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,

  FOREIGN KEY (id_doctor)     REFERENCES DOCTORES(id_doctor)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,

  FOREIGN KEY (id_trabajador) REFERENCES TRABAJADORES_SOCIALES(id_trabajador)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,

  FOREIGN KEY (id_sala)       REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,

  FOREIGN KEY (id_cama, id_sala) REFERENCES CAMAS(id_cama, id_sala)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS ix_visitas_estado   ON VISITAS_EMERGENCIA(estado, id_sala);
CREATE INDEX IF NOT EXISTS ix_visitas_doctor   ON VISITAS_EMERGENCIA(id_doctor, estado);
CREATE INDEX IF NOT EXISTS ix_visitas_paciente ON VISITAS_EMERGENCIA(id_paciente);
CREATE INDEX IF NOT EXISTS ix_visitas_folio    ON VISITAS_EMERGENCIA(folio);

-- ====================================================
-- 7) CONSECUTIVOS (contador para folios por sala)
-- ====================================================
CREATE TABLE IF NOT EXISTS CONSECUTIVOS (
  id_sala     INTEGER PRIMARY KEY,
  consecutivo INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (id_sala) REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

-- =====================================================
-- 8) Trigger para generar el FOLIO automáticamente
-- =====================================================
CREATE TRIGGER IF NOT EXISTS trg_visita_folio
AFTER INSERT ON VISITAS_EMERGENCIA
FOR EACH ROW
BEGIN
  -- Aumentar consecutivo
  UPDATE CONSECUTIVOS
    SET consecutivo = consecutivo + 1
    WHERE id_sala = NEW.id_sala;

  -- Generar folio si no se pasó uno
  UPDATE VISITAS_EMERGENCIA
    SET folio = COALESCE(
                  NEW.folio,
                  (
                    SELECT
                      CAST(NEW.id_paciente AS TEXT) || '-' ||
                      CAST(NEW.id_doctor AS TEXT)   || '-' ||
                      CAST(NEW.id_sala AS TEXT)     || '-' ||
                      printf('%04d', (SELECT consecutivo FROM CONSECUTIVOS WHERE id_sala = NEW.id_sala))
                  )
                )
    WHERE id_visita = NEW.id_visita;
END;

-- =====================================================
-- 9) LOG_REPLICACION (para sincronización entre nodos)
-- =====================================================
CREATE TABLE IF NOT EXISTS LOG_REPLICACION (
  id_log         INTEGER PRIMARY KEY AUTOINCREMENT,
  tipo_operacion TEXT NOT NULL,
  entidad        TEXT NOT NULL,
  id_entidad     TEXT,
  payload        TEXT,
  estado         TEXT NOT NULL DEFAULT 'PENDIENTE',  -- PENDIENTE | ENVIADO | APLICADO
  origen_sala    INTEGER,
  fecha_hora     TEXT NOT NULL,
  FOREIGN KEY (origen_sala) REFERENCES SALAS(id_sala)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

-- ====================================
-- 10) Datos iniciales para Sala 1 Maestra
-- ====================================
INSERT OR IGNORE INTO SALAS(id_sala, nombre, ip_sala, es_maestro, activa)
VALUES (1, 'Sala 1', '192.168.10.11', 1, 1);

INSERT OR IGNORE INTO CONSECUTIVOS(id_sala, consecutivo)
VALUES (1, 0);
