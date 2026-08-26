# Wealth Tracker + Investment Agent — Architecture Planning Prompt

Quiero evolucionar este repositorio hacia un **Personal Investment / Wealth Management Agent modular y reutilizable**, sin romper los workflows que ya funcionan.

## Modo de trabajo

Trabajá primero estrictamente en **PLAN MODE**.

Antes de escribir o modificar código:

1. Inspeccioná la estructura completa del repositorio.
2. Identificá los scripts, módulos, workflows de GitHub Actions, integración con Google Sheets, Telegram e IOL existentes.
3. Explicá cómo funciona actualmente el flujo.
4. Identificá dependencias, duplicaciones y puntos que convenga modularizar.
5. Proponé una arquitectura objetivo.
6. Dividí la implementación en fases pequeñas y testeables.
7. Indicá exactamente qué archivos crearías, cuáles modificarías y cuáles dejarías intactos.
8. No implementes nada hasta que yo apruebe el plan.

---

# Contexto actual

El proyecto ya tiene un sistema funcional relacionado con inversiones.

Actualmente existen, entre otros, componentes similares a:

* `src/jobs/watchlist_job.py`
* `src/jobs/executor.py`
* GitHub Actions para ejecutar jobs
* integración con Google Sheets
* integración con Telegram
* autenticación/API de InvertirOnline (IOL)
* detección de oportunidades CEDEAR ARS vs especie D
* referencias MEP / CCL
* `pending_trades`
* `trade_id`
* dry run
* preparación para ejecución semimanual desde GitHub Actions

El sistema actual de **watchlist intradía debe permanecer conceptualmente separado del sistema de portfolio/wealth management**.

No quiero convertir todo en un único bot monolítico.

---

# Arquitectura conceptual deseada

Quiero separar el sistema en tres motores.

## 1. FX / ARS-D Intraday Watchlist

Objetivo:

Detectar oportunidades de corto plazo relacionadas con:

* CEDEAR ARS vs CEDEAR D
* MEP
* CCL
* tipo de cambio implícito
* mejor alternativa disponible para dolarizar pesos
* liquidez disponible
* spread
* costos estimados
* cantidad realmente ejecutable

Frecuencia:

* intradía / durante mercado

Salida:

* Telegram únicamente cuando exista una oportunidad suficientemente interesante
* Top 1–2 oportunidades
* posibilidad de DRY RUN
* eventualmente EXEC semimanual

Este motor debe seguir siendo independiente del Wealth Tracker.

---

## 2. Tactical Investment Pulse

Nuevo desarrollo.

Objetivo:

Administrar únicamente el **cluster táctico / trading / satellite portfolio**.

La idea es poder destinar una parte limitada del patrimonio a posiciones más activas y medir objetivamente si esta actividad genera valor.

Frecuencia deseada:

* aproximadamente 2 veces por semana

Ejemplo:

* martes
* jueves

Debe analizar:

### Capital táctico

* capital asignado al Tactical Sleeve
* capital invertido
* cash disponible
* porcentaje del patrimonio total
* porcentaje objetivo
* exceso o déficit respecto al target

### Posiciones tácticas

Por posición:

* ticker
* cantidad
* costo promedio
* precio actual
* P&L abierto
* P&L %
* tamaño de posición
* peso dentro del Tactical Sleeve
* fecha de entrada
* tesis / estrategia si existe
* estado sugerido:

  * HOLD
  * REVIEW
  * TAKE PARTIAL
  * EXIT
  * ADD
  * NO ACTION

Las recomendaciones deben ser interpretables y nunca basarse únicamente en variaciones porcentuales de precio.

### Performance

Calcular, cuando exista información suficiente:

* P&L realizado
* P&L abierto
* P&L total
* retorno sobre capital táctico
* MTD
* YTD
* win rate
* average win
* average loss
* expectancy
* capital promedio utilizado

Separar resultados por estrategia cuando sea posible:

* FX / ARS-D arbitrage
* swing
* tactical equity
* crypto
* event-driven
* otras estrategias

---

# Tactical → Core Capital Sweep

Quiero incorporar una regla explícita para evitar que el portfolio táctico crezca indefinidamente.

Concepto:

El Tactical Sleeve tiene un peso objetivo dentro del patrimonio total.

Ejemplo conceptual:

* Core: 70%
* Convictions: 15%
* Tactical: 10%
* Cash / Reserve: 5%

Estos porcentajes NO deben quedar hardcodeados.

Deben provenir de configuración.

Regla conceptual:

```text
tactical_target_usd =
    total_portfolio_usd * tactical_target_pct

tactical_excess =
    tactical_current_usd - tactical_target_usd
```

Si:

```text
tactical_current < tactical_target
```

las ganancias pueden permanecer dentro del Tactical Sleeve.

Si:

```text
tactical_current ≈ tactical_target
```

no hay necesidad de mover capital.

Si:

```text
tactical_current > tactical_target
```

el sistema debería identificar el excedente como:

```text
potential_core_sweep
```

y sugerir cuánto podría transferirse hacia Core.

No ejecutar automáticamente ese movimiento.

Debe ser una recomendación.

También quiero poder medir:

```text
tactical_profit_generated
tactical_profit_reinvested
tactical_profit_transferred_to_core
```

Esto permitirá determinar con el tiempo si la actividad táctica realmente está agregando valor.

---

# 3. Monthly Wealth Review

Nuevo desarrollo.

Objetivo:

Evaluar la evolución patrimonial completa.

Frecuencia:

* una vez por mes

No debe comportarse como un sistema de trading.

Debe responder principalmente:

> ¿Estoy construyendo patrimonio de acuerdo con mis objetivos?

Debe analizar:

## Portfolio

* patrimonio total USD
* cambio mensual
* cambio YTD
* aportes del mes
* aportes acumulados
* rentabilidad del portfolio
* separar crecimiento por:

  * aportes
  * market performance
  * tactical performance

## Allocation

Comparar:

```text
actual allocation
vs
target allocation
```

Por ejemplo:

* Core
* Convictions
* Tactical
* Cash
* Bonds
* Crypto
* otros clusters configurables

Detectar:

* overweight
* underweight
* concentración
* desvíos relevantes

## Goals

Incluir objetivos financieros configurables.

Ejemplo:

```text
Goal: USD 100,000
Current: USD X
Progress: XX%
```

Calcular escenarios cuando sea razonable:

* capital actual
* aporte mensual
* retorno esperado configurable
* estimación de tiempo hasta objetivo

No presentar proyecciones como garantías.

## Roadmap

Poder responder:

* progreso actual
* próximos milestones
* velocidad de acumulación
* cambios relevantes respecto del mes anterior

## Recomendaciones

Generar pocas acciones de alto valor.

Ejemplos:

* priorizar próximo aporte hacia Core
* reducir Tactical
* aumentar Cash
* revisar concentración
* no hacer cambios

Evitar recomendaciones por el simple hecho de que algo subió o bajó.

---

# Wealth Tracker como fuente central

Existe un Wealth Tracker que contiene o puede contener conceptos como:

* Inputs
* Portfolio
* Movements
* Allocation
* Roadmap
* Trading Journal
* Monthly Review
* Dashboard

Quiero analizar si Google Sheets puede funcionar como **backend operativo central**, manteniendo el archivo Excel como template portable/exportable.

Evaluá esta arquitectura:

```text
Wealth Tracker Template
        ↓
Google Sheets
        ↓
Investment Agent
        │
        ├── Tactical Pulse
        ├── Monthly Wealth Review
        └── Trading Journal
```

El Intraday Watchlist utiliza parte de la misma infraestructura, pero debe permanecer como módulo independiente.

---

# Portabilidad / Multi-user

Un objetivo importante es que en el futuro otra persona pueda utilizar este proyecto.
No quiero parámetros personales hardcodeados dentro del código.

Quiero estudiar una capa de configuración similar a:

```text
Agent Config
```

que pueda incluir:

```text
base_currency
timezone

portfolio_goal_usd
monthly_contribution

core_target_pct
conviction_target_pct
tactical_target_pct
cash_target_pct

trade_target_usd
max_trade_usd
watchlist_enabled
tactical_pulse_enabled
monthly_review_enabled

tactical_pulse_days
monthly_review_day

min_edge
min_liquidity_buffer

telegram_enabled
```

Evaluá cuáles deberían estar:

1. en Google Sheets;
2. como variables de entorno;
3. como GitHub Secrets;
4. en archivos de configuración del repo.

Nunca guardar credenciales, passwords o tokens en Google Sheets ni dentro del repositorio.

---

# Data model

Antes de programar, proponé un modelo de datos claro.

Analizá si hacen falta hojas/tablas como:

```text
agent_config
portfolio
movements
watchlist
opportunities
pending_trades
trading_journal
tactical_positions
monthly_snapshots
goals
```

No agregues tablas innecesarias.

Preferí reutilizar las que ya existan.

Definí para cada tabla:

* propósito
* columnas necesarias
* primary/logical key
* quién escribe
* quién lee
* frecuencia de actualización

---

# Trading Journal

Quiero que el Trading Journal termine siendo la fuente para evaluar objetivamente el Tactical Sleeve.

Idealmente una operación ejecutada podría fluir así:

```text
Watchlist
   ↓
Pending Trade
   ↓
Dry Run
   ↓
Execution
   ↓
Trading Journal
```

Una señal que NO se ejecutó no debería convertirse automáticamente en una operación realizada.

Separar claramente:

```text
Opportunity
Trade
Position
Movement
```

No mezclar estos conceptos.

---

# Workflows deseados

Evaluar una estructura similar a:

```text
.github/workflows/

watchlist.yml
execute_trade.yml
tactical_pulse.yml
monthly_wealth_review.yml
```

### watchlist.yml

Frecuencia:
intraday.

### execute_trade.yml

Trigger:
manual `workflow_dispatch`.

### tactical_pulse.yml
Frecuencia:
2 veces por semana.

Ejemplo:
martes + jueves.

### monthly_wealth_review.yml

Frecuencia:
1 vez por mes.

Debe existir también `workflow_dispatch` para poder probar manualmente cada workflow.

---

# Telegram

Evitar ruido.

## Watchlist

Enviar solamente oportunidades accionables.

## Tactical Pulse

Enviar resumen compacto dos veces por semana.

Ejemplo conceptual:

```text
TACTICAL PULSE

Capital tactical: USD X
P&L abierto: +X
P&L realizado MTD: +X

Posiciones:
JPM +4.2% → HOLD
MU +11.3% → REVIEW PARTIAL
EWZ -2.1% → HOLD

Tactical target: 10%
Actual: 11.4%

Potential Core Sweep: USD X

Actions:
1. Review MU
2. Move up to USD X toward Core
```

## Monthly Review

Enviar una visión mucho más patrimonial:

```text
MONTHLY WEALTH REVIEW

Portfolio: USD X
Monthly change: +X%
Contributions: USD X

Goal USD 100k:
XX%

Allocation:
Core X%
Convictions X%
Tactical X%
Cash X%

Tactical contribution this month:

Suggested actions:
...
```

Los mensajes reales deberán ser compactos y fáciles de leer en Telegram.

---

# Arquitectura de código

Quiero evitar lógica duplicada entre jobs.

Evaluá extraer componentes reutilizables.

Ejemplo conceptual:

src/
***
README: ai-invest-agent — Four-Bucket Wealth Tracker & Tactical Engine
***

Resumen
-------
Este repositorio mantiene las capacidades existentes (watchlist intradía, executor, portfolio daily) e introduce una arquitectura orientada a 4 buckets: `RESERVE`, `CORE`, `CONVICTION`, `TACTICAL`.

Objetivos clave
- Separar intención (bucket) de tipo de instrumento (`tipo`).
- Mantener el Watchlist intradía independiente.
- Implementar un `Tactical Pulse` operativo (dry-run) y un `Monthly Wealth Review` patrimonial.
- Centralizar precios y estado de portfolio en `src/common/pricing.py` y `src/common/portfolio_state.py`.

Quick start
-----------
1. Agregar Secrets en GitHub: `PORTFOLIO_GS_CREDS`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`. Opcional: `IOL_USERNAME`, `IOL_PASSWORD`.
2. Ejecutar workflow `Tactical Pulse Dry-Run` en Actions o localmente:

```powershell
python -m src.scripts.setup_agent_config
python -m src.scripts.setup_example_sheets
python -m src.jobs.tactical_pulse_job
```

Sheets (hojas) mínimas
----------------------
- `agent_config` — key, value, description (configuración de objetivos y sweep).
- `portfolio` — conservar `tipo` (compatibilidad diaria). Añadir `bucket`/`strategy` para intención:
  `ticker,tipo,cantidad,ppc,last_price,ratio,bucket,strategy`
- `trading_journal` — registro de trades y ejecuciones.
- `tactical_positions` — snapshot de positions tácticas.
- `prices_daily` — precios diarios para fallback/manual overrides.
- `pending_trades`, `portfolio_history_v2`, `watchlist_history_v2` — existentes.

Data model mínimo por posición
-----------------------------
```
ticker,name,instrument_type,bucket,quantity,avg_cost,current_price,market_value_usd,portfolio_weight,bucket_weight,target_weight,max_weight,realized_pnl,unrealized_pnl,thesis,thesis_status,entry_date,expected_horizon
```

Behavior & rules
----------------
- `bucket` expresa intención (RESERVE/CORE/CONVICTION/TACTICAL). No sobrescribir `tipo`.
- Tactical → Core sweep: configurable (por defecto 50% de ganancias realizadas → Core).
- Pricing priority: `portfolio.last_price` override -> `prices_daily` -> IOL -> Yahoo USD.

Roadmap (incremental)
----------------------
Phase 1 — Foundation: `pricing.py`, `portfolio_state.py` (implementado).
Phase 2 — Tactical engine: improve tactical metrics, sweep suggestions (scaffold present).
Phase 3 — Conviction engine & allocation rules.
Phase 4 — Monthly wealth review and snapshots.
Phase 5 — Tests, CI, docs, migration scripts.

Notes
-----
- Todas las recomendaciones son sugerencias: no ejecutar movimientos automáticamente.
- Preserve audit trail: append actions/recommendations to `trading_journal`.

Si querés, actualizo este README con ejemplos CSV para copiar/pegar en Sheets (agent_config, portfolio, trading_journal, tactical_positions). Hago eso ahora si confirmás.
Si querés, actualizo este README con ejemplos CSV para copiar/pegar en Sheets (agent_config, portfolio, trading_journal, tactical_positions). Hago eso ahora si confirmás.

Example CSVs
------------
He incluido ejemplos listos para copiar/pegar en `Google Sheets` dentro de `docs/examples/`:

- [agent_config_example.csv](docs/examples/agent_config_example.csv)
- [portfolio_example.csv](docs/examples/portfolio_example.csv)
- [trading_journal_example.csv](docs/examples/trading_journal_example.csv)
- [tactical_positions_example.csv](docs/examples/tactical_positions_example.csv)

Copialos directamente en tus hojas o súbelos como referencia para backfill.
    clients/
        iol_client.py
        sheets_client.py
        telegram_client.py

    services/
        market_data.py
        fx_service.py
        portfolio_service.py
        tactical_service.py
        allocation_service.py
        performance_service.py

    repositories/
        portfolio_repository.py
        trades_repository.py
        config_repository.py

    jobs/
        watchlist_job.py
        executor.py
        tactical_pulse_job.py
        monthly_wealth_review_job.py
```

Esto es solamente una hipótesis.

No la implementes automáticamente.

Primero analizá si tiene sentido según el repo actual.

Preferí cambios incrementales sobre un refactor masivo.

---

# Principios técnicos

Priorizar:

* simplicidad
* trazabilidad
* idempotencia
* separación de responsabilidades
* configuración externa
* testabilidad
* observabilidad
* posibilidad de dry-run
* logs claros
* degradación segura ante APIs caídas

Evitar:

* mega scripts
* lógica financiera duplicada
* valores hardcodeados
* recomendaciones no explicables
* automatizar movimientos patrimoniales sin confirmación
* dependencia innecesaria de APIs frágiles

---

# Seguridad financiera

Especialmente para operaciones:

* ningún análisis debe ejecutar una operación automáticamente;
* ejecución debe permanecer separada de recomendación;
* mantener confirmación explícita para live;
* utilizar órdenes límite;
* revalidar mercado inmediatamente antes de ejecutar;
* contemplar partial fills;
* contemplar fallo de segunda pata;
* registrar IDs de órdenes;
* mantener audit trail.

El Wealth Agent puede recomendar rebalanceos o Core Sweeps, pero inicialmente NO debe ejecutarlos automáticamente.

---

# Testing

Diseñá desde el principio una estrategia de testing.

Quiero poder ejecutar:

```text
DRY RUN tactical pulse
DRY RUN monthly review
```

sin modificar datos productivos.

Proponé:

* unit tests
* fixtures
* mock de IOL
* mock de Google Sheets
* sample portfolio
* sample trading journal

Identificá qué cálculos financieros deben tener tests obligatoriamente.

---

# Fases de implementación

Proponé un roadmap incremental aproximadamente así, pero ajustalo según el código real encontrado.

### Phase 0 — Repository analysis

Entender qué existe hoy.

### Phase 1 — Shared foundation

Config + clients/helpers reutilizables, solamente cuando sea necesario.

### Phase 2 — Tactical data model

Definir Tactical Sleeve + Trading Journal + performance.

### Phase 3 — Tactical Pulse

Crear análisis + Telegram + workflow.

### Phase 4 — Monthly snapshots

Capturar snapshots históricos del portfolio.

### Phase 5 — Monthly Wealth Review

Goals + allocation + performance + roadmap.

### Phase 6 — Integration with executed trades

Registrar automáticamente trades efectivamente ejecutados.

### Phase 7 — Portable user configuration

Permitir crear una copia del sistema para otro usuario con mínima configuración.

---

# Primera tarea

Ahora NO escribas código.

Primero inspeccioná el repositorio y entregame:

## A. Current State

Mapa del proyecto actual.

## B. Existing Flow

Explicación del flujo actual:

```text
market data
→ watchlist
→ pending trade
→ dry run
→ execution
```

según lo que realmente encuentres.

## C. Gaps

Qué falta para llegar al Investment Agent descrito arriba.

## D. Proposed Architecture

Arquitectura recomendada utilizando la menor cantidad de cambios innecesarios.

## E. Data Model

Tablas / sheets existentes y nuevas.

## F. Files Impacted

Tabla:

| File | Action                 | Reason |
| ---- | ---------------------- | ------ |
| ...  | keep / modify / create | ...    |

## G. Implementation Plan

Fases pequeñas, ordenadas y testeables.

## H. Decisions Needed

Preguntas o decisiones que necesites que yo tome antes de implementar.

## I. First Milestone

Recomendame cuál debería ser el primer milestone que produzca valor real y pueda probarse sin riesgo.

No implementes todavía.

Esperá mi aprobación del plan.
