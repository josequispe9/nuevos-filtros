import requests
import subprocess
import time
import sys
import os
from pathlib import Path
from datetime import datetime

# ==================== CONFIGURACIÓN ====================
TOKEN = "8490086356:AAE1dlwognbqUlZIgyGU0m_iU8MOhkb-INU"
CHAT_ID = "1874753772"
BASE_DIR = Path(__file__).parent.parent  # Subir un nivel desde telegram-bot/

# Scripts a ejecutar en orden
SCRIPTS = [
    "filtros/1_generar-archivos-filtrado.py",
    "filtros/2_Filtro-seleccion-de-lote.py",
    "filtros/3_Formato-base.py"
]

# Archivo final esperado
OUTPUT_PATTERN = "data/bases/BASE_FINAL_*.csv"


# ==================== FUNCIONES ====================
def send_message(text):
    """Envía un mensaje al usuario por Telegram"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=data)
    except Exception as e:
        print(f"Error enviando mensaje: {e}")


def send_document(file_path):
    """Envía un archivo al usuario por Telegram"""
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    try:
        # Verificar tamaño del archivo (Telegram límite: 50MB para bots)
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        if file_size_mb > 50:
            send_message(f"⚠️ El archivo es muy grande ({file_size_mb:.1f}MB). Telegram permite máximo 50MB.\n\n📍 Ubicación del archivo:\n`{file_path}`")
            return None

        with open(file_path, 'rb') as file:
            files = {'document': file}
            data = {'chat_id': CHAT_ID}
            # Aumentar timeout para archivos grandes
            response = requests.post(url, files=files, data=data, timeout=300)

            if response.status_code == 200:
                return response.json()
            else:
                send_message(f"❌ Error del servidor: {response.status_code}\n{response.text[:200]}")
                return None
    except requests.exceptions.Timeout:
        send_message(f"⏱️ Timeout al enviar archivo. El archivo está listo en:\n`{file_path}`")
    except Exception as e:
        print(f"Error enviando archivo: {e}")
        send_message(f"❌ Error enviando archivo ({file_size_mb:.1f}MB): {type(e).__name__}\n\n📍 El archivo está disponible en:\n`{file_path}`")


def execute_pipeline():
    """Ejecuta los 3 scripts en secuencia"""
    send_message("🚀 *Iniciando pipeline de filtrado*\n\n⏳ Este proceso puede tardar varios minutos...")

    start_time = time.time()

    for i, script in enumerate(SCRIPTS, 1):
        script_name = Path(script).name
        send_message(f"📄 *Script {i}/3:* `{script_name}`\n⏳ Ejecutando...")

        try:
            # Ejecutar script usando el mismo Python que ejecuta este bot
            # Configurar variables de entorno para forzar UTF-8
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'

            result = subprocess.run(
                [sys.executable, script],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minutos de timeout por script
                env=env,
                encoding='utf-8',
                errors='replace'  # Reemplaza caracteres no decodificables
            )

            if result.returncode == 0:
                send_message(f"✅ *Script {i}/3 completado:* `{script_name}`")
            else:
                error_msg = result.stderr[-500:] if result.stderr else "Error desconocido"
                send_message(f"❌ *Error en script {i}/3:* `{script_name}`\n\n```\n{error_msg}\n```")
                send_message("⚠️ Pipeline detenido debido al error")
                return False

        except subprocess.TimeoutExpired:
            send_message(f"⏱️ *Timeout en script {i}/3:* `{script_name}`\n\nEl script superó los 10 minutos de ejecución")
            return False
        except Exception as e:
            send_message(f"❌ *Error ejecutando script {i}/3:* `{script_name}`\n\n```\n{str(e)}\n```")
            return False

    # Calcular tiempo total
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    send_message(f"✅ *Pipeline completado exitosamente*\n\n⏱️ Tiempo total: {minutes}m {seconds}s")

    # Buscar y enviar archivo final
    try:
        output_dir = BASE_DIR / "data" / "bases"
        output_files = list(output_dir.glob("BASE_FINAL_*.csv"))
        if output_files:
            # Ordenar por fecha de modificación, tomar el más reciente
            latest_file = max(output_files, key=lambda p: p.stat().st_mtime)
            send_message(f"📎 *Enviando archivo final:* `{latest_file.name}`")
            send_document(latest_file)
            send_message("🎉 *Proceso completado!*")
        else:
            send_message(f"⚠️ Pipeline completado pero no se encontró el archivo final en {output_dir}")
    except Exception as e:
        send_message(f"❌ Error buscando/enviando archivo: {str(e)}")

    return True


def get_updates(offset=None):
    """Obtiene actualizaciones del bot"""
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    params = {"timeout": 30, "offset": offset}
    try:
        response = requests.get(url, params=params, timeout=35)
        return response.json()
    except Exception as e:
        print(f"Error obteniendo actualizaciones: {e}")
        return None


def main():
    """Función principal del bot"""
    print("🤖 Bot de Telegram iniciado")
    print(f"👤 Escuchando mensajes del Chat ID: {CHAT_ID}")
    print("📱 Envía /start para ejecutar el pipeline\n")

    send_message("🤖 *Bot activado*\n\nEnvía /start para ejecutar el pipeline de filtrado")

    offset = None

    while True:
        try:
            updates = get_updates(offset)

            if updates and updates.get("ok"):
                for update in updates.get("result", []):
                    offset = update["update_id"] + 1

                    # Verificar si hay un mensaje
                    if "message" not in update:
                        continue

                    message = update["message"]
                    chat_id = str(message["chat"]["id"])
                    text = message.get("text", "")

                    # Solo responder al chat autorizado
                    if chat_id != CHAT_ID:
                        continue

                    print(f"📩 Mensaje recibido: {text}")

                    # Procesar comando /start
                    if text == "/start":
                        print("🚀 Ejecutando pipeline...")
                        execute_pipeline()
                    else:
                        send_message("ℹ️ Comando no reconocido.\n\nUsa /start para ejecutar el pipeline")

            time.sleep(1)

        except KeyboardInterrupt:
            print("\n\n👋 Bot detenido por el usuario")
            send_message("🛑 *Bot detenido*")
            break
        except Exception as e:
            print(f"❌ Error en el loop principal: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
