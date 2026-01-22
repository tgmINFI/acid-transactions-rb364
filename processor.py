import sqlite3

class ShipmentProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def process_shipment(self, item_name, quantity, log_callback):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        log_callback(f"--- STARTING TRANSACTION: Move {quantity} of {item_name} ---")

        try:
            # SCHRITT 1: Inventar abziehen
            # Wenn der Bestand negativ werden würde, wirft sqlite3 hier eine IntegrityError
            cursor.execute("UPDATE inventory SET stock_qty = stock_qty - ? WHERE item_name = ?", 
                           (quantity, item_name))
            log_callback(">> STEP 1 SUCCESS: Inventory Deducted.")

            # SCHRITT 2: Den Versand loggen
            # Dieser Schritt wird NUR ausgeführt, wenn Schritt 1 keine Exception geworfen hat
            cursor.execute("INSERT INTO shipment_log (item_name, qty_moved) VALUES (?, ?)", 
                           (item_name, quantity))
            log_callback(">> STEP 2 SUCCESS: Shipment Logged.")

            # Wenn wir hier ankommen, waren beide Schritte erfolgreich
            conn.commit()
            log_callback("--- TRANSACTION COMMITTED SUCCESSFULLY ---")

        except sqlite3.Error as e:
            # Falls IRGENDEIN Fehler auftritt (z.B. zu wenig Bestand in Schritt 1),
            # machen wir alle bisherigen Änderungen in dieser Transaktion rückgängig.
            conn.rollback()
            log_callback(f">> ERROR DETECTED: {e}")
            log_callback(">> TRANSACTION ROLLED BACK: No data was changed.")

        finally:
            # Die Verbindung muss immer geschlossen werden
            conn.close()