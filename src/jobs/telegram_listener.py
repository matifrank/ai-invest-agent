import os
import time
import logging
import requests
import telebot

logging.basicConfig(
    level=logging.INFO,
    
    format="%(asctime)s | %(levelname)s | %(message)s"
)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
ALLOWED_CHAT_ID = str(os.environ["TELEGRAM_CHAT_ID"])

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_OWNER = os.environ["GITHUB_OWNER"]
GITHUB_REPO = os.environ["GITHUB_REPO"]
GITHUB_WORKFLOW_ID = os.environ.get("GITHUB_WORKFLOW_ID", "execute_trade.yml")
GITHUB_REF = os.environ.get("GITHUB_REF", "main")

bot = telebot.TeleBot(TELEGRAM_TOKEN, parse_mode=None)


def is_allowed_chat(message) -> bool:
    return str(message.chat.id) == ALLOWED_CHAT_ID


def trigger_github_workflow(trade_id: str, mode: str):
    url = (
        f"https://api.github.com/repos/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW_ID}/dispatches"
    )

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    payload = {
        "ref": GITHUB_REF,
        "inputs": {
            "trade_id": trade_id,
            "mode": mode,
        }
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        if r.status_code in (200, 201, 204):
            return True, "Workflow disparado correctamente."
        return False, f"GitHub API {r.status_code}: {r.text}"
    except Exception as e:
        return False, f"Error llamando GitHub API: {e}"


def parse_trade_command(text: str):
    if not text:
        return None

    clean = text.strip()
    if not clean:
        return None

    parts = clean.split(maxsplit=1)
    if len(parts) != 2:
        return None

    cmd = parts[0].upper().strip()
    trade_id = parts[1].strip()

    if cmd not in ("DRY", "EXEC"):
        return None

    return cmd, trade_id


@bot.message_handler(commands=["start", "help"])
def handle_help(message):
    if not is_allowed_chat(message):
        return

    help_text = (
        "Bot de confirmación de trades.\n\n"
        "Comandos:\n"
        "- PING\n"
        "- DRY <trade_id>\n"
        "- EXEC <trade_id>\n\n"
        "Ejemplo:\n"
        "DRY AAPL_1774028748_138"
    )
    bot.reply_to(message, help_text)


@bot.message_handler(func=lambda m: True)
def handle_message(message):
    if not is_allowed_chat(message):
        logging.warning("Mensaje ignorado de chat no autorizado: %s", message.chat.id)
        return

    text = (message.text or "").strip()
    upper = text.upper()
    logging.info("Mensaje recibido: %s", text)

    if upper == "PING":
        bot.reply_to(message, "✅ Bot activo.")
        return

    parsed = parse_trade_command(text)
    if not parsed:
        bot.reply_to(
            message,
            "No entendí el comando.\nUsá:\nDRY <trade_id>\nEXEC <trade_id>\nPING"
        )
        return

    cmd, trade_id = parsed
    mode = "dry_run" if cmd == "DRY" else "live"

    bot.reply_to(message, f"🚀 Enviando {cmd} para {trade_id}...")
    ok, detail = trigger_github_workflow(trade_id, mode)

    if ok:
        bot.send_message(
            message.chat.id,
            f"✅ {cmd} enviado para `{trade_id}`.\n{detail}",
            parse_mode="Markdown"
        )
    else:
        bot.send_message(
            message.chat.id,
            f"❌ No se pudo disparar `{trade_id}`.\n{detail}",
            parse_mode="Markdown"
        )


def main():
    logging.info("Iniciando listener Telegram...")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30)
        except Exception as e:
            logging.exception("Fallo polling, reintentando en 5s: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()