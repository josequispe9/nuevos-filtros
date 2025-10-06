import requests

TOKEN = "8490086356:AAE1dlwognbqUlZIgyGU0m_iU8MOhkb-INU"

print("🔍 Buscando tu Chat ID...")
print("📱 Envía cualquier mensaje a tu bot en Telegram y presiona Enter aquí")
input()

url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
response = requests.get(url)
data = response.json()

if data['result']:
    for update in data['result']:
        if 'message' in update:
            chat_id = update['message']['chat']['id']
            username = update['message']['chat'].get('username', 'Sin username')
            first_name = update['message']['chat'].get('first_name', '')

            print(f"\n✅ Chat ID encontrado: {chat_id}")
            print(f"👤 Nombre: {first_name}")
            print(f"🔖 Username: @{username}")
            print(f"\n📋 Usa este Chat ID: {chat_id}")
            break
else:
    print("\n⚠️ No se encontraron mensajes. Asegúrate de:")
    print("1. Haber enviado un mensaje a tu bot")
    print("2. Que el token sea correcto")
