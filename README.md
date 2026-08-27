# AI Invest Agent

Bot de análisis y monitoreo de cartera para CEDEARs.

## Qué hace

- Lee la cartera y la watchlist desde Google Sheets.
- Consulta cotizaciones de IOL y fallback con Yahoo Finance.
- Calcula valor implícito en USD usando CCL.
- Detecta oportunidades de arbitraje entre ARS y D.
- Guarda historial en Sheets y envía alertas por Telegram.

## Requisitos

- Python 3.10+
- Dependencias del archivo `requirements.txt`
- Variables de entorno definidas en el shell o `.env`

## Variables de entorno

```bash
PORTFOLIO_GS_CREDS='{"type": "service_account", ...}'
SPREADSHEET_NAME='ai-portfolio-agent'

IOL_USERNAME='tu_usuario'
IOL_PASSWORD='tu_password'

TELEGRAM_TOKEN='123456:ABCDEF'
TELEGRAM_CHAT_ID='-1001234567890'

BROKER_FEE_PCT='0.5'
WATCH_MIN_DIFF_PCT='1.0'
WATCH_MIN_NET_USD_PER_CEDEAR='0.12'
TARGET_USD='300'
MIN_MONTO_OPERADO_ARS='0'
MIN_TOP_QTY_ARS='1'
MIN_TOP_QTY_D='1'
USE_TIME_WINDOW='0'
PORTFOLIO_PRICE_MODE='mark'
```

## Ejecución

```bash
python main.py
```

## Archivos principales

- [main.py](main.py): entrypoint mínimo que delega al refactor.
- [main_refactor.py](main_refactor.py): lógica principal del pipeline.
- [src/common/](src/common): utilidades de sheets, IOL, Yahoo, Telegram y cálculos.

## Notas

- El proyecto está pensado para ejecución periódica y análisis de oportunidades financieras.
- Si no hay credenciales de IOL, el flujo usa Yahoo Finance como respaldo.
