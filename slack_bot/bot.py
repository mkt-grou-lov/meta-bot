"""
Slack Bot - Meta Ads Assistant
Escuta mencoes no Slack e responde com metricas do Meta Ads via Claude.
"""

import os, re, json, logging
from datetime import datetime
import httpx, anthropic
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
META_ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
META_AD_ACCOUNT_ID = os.environ["META_AD_ACCOUNT_ID"]
META_API_BASE = "https://graph.facebook.com/v21.0"
CORE_FIELDS = "campaign_name,adset_name,impressions,reach,clicks,spend,cpm,cpc,ctr,actions,cost_per_action_type,frequency"

PERIOD_ALIASES = {
    r"hoje": "today", r"ontem": "yesterday",
    r"ultimos?\s*7\s*dias?": "last_7d", r"ultimos?\s*14\s*dias?": "last_14d",
    r"ultimos?\s*30\s*dias?": "last_30d", r"este\s*mes": "this_month",
    r"mes\s*passado": "last_month", r"este\s*ano": "this_year",
}

SYSTEM_PROMPT = """Voce e o assistente estrategico de Meta Ads da Grou, uma HR Tech brasileira especializada em gestao comportamental e distribuidora exclusiva do PDA Assessment no Brasil.

CONTEXTO DA GROU
Posicionamento: ciencia do comportamento e desenvolvimento humano.
Produtos: PDA Assessment, Feedback 360, Grou Academy, Grou Skills, Self Guru.
Publico: gestores de RH, lideres de PME, diretores de pessoas.
Contexto regulatorio: NR-1 (riscos psicossociais) e ganchos de urgencia prioritarios.

PILARES EDITORIAIS - use para classificar campanhas pelo nome:
- Ciencia do Comportamento: PDA, perfis, eixos comportamentais
- Lideranca e Soft Skills: gestao, feedback, desenvolvimento
- PDA em Acao: casos de uso, demonstracoes, prova social
- Dados que Convencem: estatisticas, benchmarks, pesquisas
- Gente por Tras da Grou: cultura, time, bastidores
- NR-1 / Compliance: riscos psicossociais, PGR, obrigatoriedade

BENCHMARKS HRTECH B2B BRASIL:
- CPL: R$ 18-45 (bom: abaixo de R$ 25)
- CTR: 0,8-2,0% (bom: acima de 1,2%)
- CPM: R$ 25-60 (bom: abaixo de R$ 35)
- Frequencia saudavel: 1,5-3,5x (acima de 4,0x = saturacao)
- Conv. lead/clique: 5-15% (bom: acima de 10%)

REGRAS DE ANALISE:
1. Ordene campanhas por investimento (maior para menor)
2. Calcule taxa de conversao: leads / cliques x 100
3. Marque com alerta se CPL for 2x acima da media
4. Compare cada metrica com benchmark e diga se esta bom/regular/atencao
5. Se frequencia acima de 4,0x, sinalize saturacao de audiencia
6. Identifique o pilar editorial de cada campanha pelo nome

FORMATO SLACK OBRIGATORIO:

*Relatorio Meta Ads - [periodo]*

*CONSOLIDADO*
Investimento: R$ X.XXX,XX
Alcance: X.XXX | Impressoes: X.XXX
Cliques: XXX | CTR: X,XX% [bom/regular/atencao]
Leads: XX | CPL medio: R$ XX,XX [bom/regular/atencao]
CPC: R$ X,XX | CPM: R$ XX,XX

*POR CAMPANHA*
[Para cada campanha: nome, gasto, alcance, leads, CPL, conversao, alertas]

*OBSERVACOES ESTRATEGICAS*
[2-3 insights conectando dados com contexto Grou e oportunidades editoriais]

Responda sempre em portugues brasileiro. Va direto aos numeros sem introducoes."""

app = None
anthropic_client = None

def parse_intent(text):
    text = re.sub(r"<@[A-Z0-9]+>", "", text).strip().lower()
    intent = {"period": "last_7d", "campaign_name": None, "since": None, "until": None, "level": "campaign"}
    m = re.search(r"desde\s+(\d{4}-\d{2}-\d{2})(?:\s+ate\s+(\d{4}-\d{2}-\d{2}))?", text)
    if m:
        intent["since"] = m.group(1)
        intent["until"] = m.group(2) or datetime.today().strftime("%Y-%m-%d")
        intent["period"] = None
    for pattern, preset in PERIOD_ALIASES.items():
        if re.search(pattern, text):
            intent["period"] = preset
            break
    c = re.search(r"campanha\s+(.+?)(?:\s+(?:desde|ultimos?|hoje|ontem|este|mes)|$)", text)
    if c:
        intent["campaign_name"] = c.group(1).strip()
    if re.search(r"\bconjunto\b|\badset\b", text):
        intent["level"] = "adset"
    elif re.search(r"\banuncio\b|\bcriativo", text):
        intent["level"] = "ad"
    return intent

def fetch_meta(intent):
    params = {"access_token": META_ACCESS_TOKEN, "fields": CORE_FIELDS, "level": intent["level"], "limit": 50}
    if intent.get("since"):
        params["time_range"] = json.dumps({"since": intent["since"], "until": intent["until"]})
    else:
        params["date_preset"] = intent.get("period", "last_7d")
    try:
        with httpx.Client(timeout=30) as client:
            r = client.get(f"{META_API_BASE}/{META_AD_ACCOUNT_ID}/insights", params=params)
            r.raise_for_status()
            rows = r.json().get("data", [])
            if intent.get("campaign_name"):
                rows = [x for x in rows if intent["campaign_name"].lower() in x.get("campaign_name","").lower()]
            return json.dumps(rows, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

def query(text):
    intent = parse_intent(text)
    period = f"{intent['since']} a {intent['until']}" if intent.get("since") else intent.get("period","last_7d")
    data = fetch_meta(intent)
    try:
        r = anthropic_client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": f"Periodo: {period}\nNivel: {intent['level']}\n\nDados Meta Ads API:\n{data}"}]
        )
        return r.content[0].text
    except Exception as e:
        return f"Erro: {e}"

def handle_mention(event, say, client):
    channel, ts = event["channel"], event["ts"]
    try: client.reactions_add(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
    except: pass
    result = query(event.get("text",""))
    try:
        client.reactions_remove(channel=channel, name="hourglass_flowing_sand", timestamp=ts)
        client.reactions_add(channel=channel, name="white_check_mark", timestamp=ts)
    except: pass
    say(text=result, thread_ts=event.get("thread_ts") or ts)

def main():
    global app, anthropic_client
    app = App(token=SLACK_BOT_TOKEN)
    anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    app.event("app_mention")(handle_mention)
    logger.info("Meta Ads Slack Bot iniciado")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()

if __name__ == "__main__":
    main()
