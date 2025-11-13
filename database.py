# database.py
import sqlite3
from contextlib import contextmanager
from datetime import datetime

# Ruta absoluta a tu base de datos
DB_PATH = "/opt/emergencias/SistemaDistribuido/data/sala1.db"

def connect(db_path: str = DB_PATH):
    """
    Abre una conexión a la base de datos.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row        # permite acceder por nombre de columna
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

@contextmanager
def tx(db_path: str = DB_PATH):
    """
    Maneja una transacción (BEGIN / COMMIT / ROLLBACK).
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE;")  # lock de escritura local
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ---------- Consultas de apoyo ----------

def get_visitas_abiertas():
    with connect() as c:
        return c.execute("""
            SELECT folio, id_paciente, id_doctor, id_sala, id_cama,
                   prioridad, motivo, fecha_hora_inicio
            FROM VISITAS_EMERGENCIA
            WHERE estado='ABIERTA'
            ORDER BY fecha_hora_inicio DESC
        """).fetchall()

# ---------- Lógica de creación/cierre de visitas ----------

def _seleccionar_doctor_disponible(conn):
    r = conn.execute("""
      SELECT d.id_doctor
      FROM DOCTORES d
      WHERE d.activo=1 AND d.disponible=1
      ORDER BY (SELECT COUNT(*) FROM VISITAS_EMERGENCIA v
                WHERE v.id_doctor=d.id_doctor AND v.estado='ABIERTA') ASC
      LIMIT 1
    """).fetchone()
    return r[0] if r else None

def _seleccionar_cama_libre(conn):
    r = conn.execute("""
      SELECT id_cama, id_sala
      FROM CAMAS
      WHERE estado='LIBRE'
      ORDER BY id_sala, id_cama
      LIMIT 1
    """).fetchone()
    return (r[0], r[1]) if r else (None, None)

def _ocupar_recursos(conn, id_doctor, id_cama, id_sala_cama):
    cur = conn.execute(
        "UPDATE DOCTORES SET disponible=0 WHERE id_doctor=? AND disponible=1",
        (id_doctor,)
    )
    if cur.rowcount != 1:
        return False

    cur = conn.execute("""
        UPDATE CAMAS
        SET estado='OCUPADA'
        WHERE id_cama=? AND id_sala=? AND estado='LIBRE'
    """, (id_cama, id_sala_cama))

    if cur.rowcount != 1:
        conn.execute("UPDATE DOCTORES SET disponible=1 WHERE id_doctor=?",
                     (id_doctor,))
        return False

    return True

def crear_visita_tx(payload: dict):
    """
    Crea una visita de emergencia dentro de una transacción.
    """
    with tx() as conn:
        id_doctor = _seleccionar_doctor_disponible(conn)
        if not id_doctor:
            raise RuntimeError("No hay doctor disponible")

        id_cama, id_sala_dest = _seleccionar_cama_libre(conn)
        if not id_cama:
            raise RuntimeError("No hay camas libres")

        if not _ocupar_recursos(conn, id_doctor, id_cama, id_sala_dest):
            raise RuntimeError("Recursos tomados, reintente")

        conn.execute("""
          INSERT INTO VISITAS_EMERGENCIA(
            folio, id_paciente, id_doctor, id_trabajador, id_sala, id_cama,
            origen_solicitud, prioridad, motivo, fecha_hora_inicio, estado
          ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            None,
            payload['id_paciente'],
            id_doctor,
            payload['id_trabajador'],
            id_sala_dest,
            id_cama,
            payload['origen_solicitud'],
            int(payload.get('prioridad', 3)),
            payload.get('motivo', ''),
            datetime.now().isoformat(timespec='seconds'),
            'ABIERTA'
        ))

        row = conn.execute("""
          SELECT folio
          FROM VISITAS_EMERGENCIA
          WHERE id_visita = (SELECT last_insert_rowid())
        """).fetchone()

        return {
            "folio": row['folio'],
            "id_doctor": id_doctor,
            "id_cama": id_cama,
            "id_sala": id_sala_dest
        }

def cerrar_visita_tx(folio: str):
    with tx() as conn:
        cur = conn.execute("""
          UPDATE VISITAS_EMERGENCIA
          SET estado='CERRADA', fecha_hora_cierre=?
          WHERE folio=? AND estado='ABIERTA'
        """, (datetime.now().isoformat(timespec='seconds'), folio))

        if cur.rowcount != 1:
            raise RuntimeError("Visita no encontrada o ya cerrada")

        conn.execute("""
          UPDATE DOCTORES
          SET disponible=1
          WHERE id_doctor = (
            SELECT id_doctor FROM VISITAS_EMERGENCIA WHERE folio=?
          )
        """, (folio,))

        conn.execute("""
          UPDATE CAMAS
          SET estado='LIBRE'
          WHERE id_cama = (
                  SELECT id_cama FROM VISITAS_EMERGENCIA WHERE folio=?
                )
            AND id_sala = (
                  SELECT id_sala FROM VISITAS_EMERGENCIA WHERE folio=?
                )
        """, (folio, folio))
