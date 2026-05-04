"""
data/rag/engine.py
==================
Report + Contract generation engine.

Contract templates extracted VERBATIM from:
  - promesse.pdf / promesseELMEJEDA.pdf (SARI — Tunisian official promesse de vente)
  - contrat-de-location-meuble-2020-pdf.pdf (French loi 6 juillet 1989 — bail meublé)

The LLM fills party/property blanks ONLY. Temperature = 0.0 for contracts.
Temperature = 0.3 for reports.
DB: singleton psycopg2 connection through PgBouncer port 6543.
"""

from __future__ import annotations

import os
import json
import textwrap
from typing import Generator, Any
from threading import Lock
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_REPORT_MODEL", "gemma2:2b")
EMBED_MODEL     = "nomic-embed-text"
TOP_K           = 8
MAX_TOKENS      = 2048

# ── Singleton DB connection ────────────────────────────────────────────────────

_PG_PARAMS = dict(
    host     = os.getenv("SUPABASE_DB_HOST", "aws-1-eu-central-1.pooler.supabase.com"),
    port     = int(os.getenv("SUPABASE_DB_PORT", "6543")),
    dbname   = os.getenv("SUPABASE_DB_NAME", "postgres"),
    user     = os.getenv("SUPABASE_DB_USER", "postgres.amxnojlfczwffvtwutrb"),
    password = os.getenv("SUPABASE_DB_PASSWORD", ""),
    sslmode  = os.getenv("SUPABASE_DB_SSLMODE", "require"),
    options  = "-c statement_timeout=30000",
)

_pg_conn: psycopg2.extensions.connection | None = None
_pg_lock = Lock()


def _get_conn() -> psycopg2.extensions.connection:
    global _pg_conn
    with _pg_lock:
        if _pg_conn is not None:
            try:
                _pg_conn.cursor().execute("SELECT 1")
                return _pg_conn
            except Exception:
                try:
                    _pg_conn.close()
                except Exception:
                    pass
                _pg_conn = None
        _pg_conn = psycopg2.connect(**_PG_PARAMS)
        _pg_conn.autocommit = True
        return _pg_conn


# ── Retrieval ──────────────────────────────────────────────────────────────────

def _embed_query(query: str) -> list[float]:
    from langchain_ollama import OllamaEmbeddings
    embedder = OllamaEmbeddings(model=EMBED_MODEL, base_url=OLLAMA_BASE_URL)
    return embedder.embed_query(query)


def retrieve_context(query: str, top_k: int = TOP_K, contract_type: str = None) -> list[dict]:
    vec = _embed_query(query)
    type_filter = ""
    if contract_type == "contrat_de_location":
        type_filter = "AND (label ILIKE '%location%' OR label ILIKE '%meuble%')"
    elif contract_type in ["promesse_de_vente", "compromis_de_vente", "acte_de_vente"]:
        type_filter = "AND (label ILIKE '%promesse%' OR label ILIKE '%vente%' OR label ILIKE '%SARI%')"
    sql = f"""
        SELECT label, content, source,
               1 - (embedding <=> %s::vector) AS similarity
        FROM report_documents
        WHERE 1=1 {type_filter}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, [vec, vec, top_k])
        rows = cur.fetchall()
    return [
        {"label": r[0], "content": r[1], "source": r[2], "similarity": float(r[3])}
        for r in rows
    ]


def _format_context(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        parts.append(
            f"[Document {i} — {c['label']} (relevance: {c['similarity']:.2f})]\n"
            f"{c['content']}"
        )
    return "\n\n---\n\n".join(parts)


# ── DB stats ───────────────────────────────────────────────────────────────────

def _get_listing_stats(filters: dict) -> dict:
    wheres = ["should_drop IS NOT TRUE", "price IS NOT NULL", "price > 0"]
    params: list[Any] = []

    period = filters.get("period", {})
    if period.get("start_date"):
        wheres.append("scraped_at::date >= %s")
        params.append(period["start_date"])
    if period.get("end_date"):
        wheres.append("scraped_at::date <= %s")
        params.append(period["end_date"])
    if filters.get("city"):
        wheres.append("LOWER(city) = LOWER(%s)")
        params.append(filters["city"])
    if filters.get("region"):
        wheres.append("LOWER(region) = LOWER(%s)")
        params.append(filters["region"])
    if filters.get("transaction_type"):
        wheres.append("transaction_type = %s")
        params.append(filters["transaction_type"])
    if filters.get("property_type"):
        wheres.append("property_type = %s")
        params.append(filters["property_type"])

    where_sql = " AND ".join(wheres)
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)::int,
                   AVG(price)::numeric(14,0), MIN(price)::numeric(14,0),
                   MAX(price)::numeric(14,0),
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::numeric(14,0),
                   AVG(price_per_m2)::numeric(8,0), AVG(surface)::numeric(8,0),
                   AVG(rooms)::numeric(4,1)
            FROM listings WHERE {where_sql}
        """, params)
        row = cur.fetchone()
        cur.execute(f"""
            SELECT city, COUNT(*) as cnt, AVG(price)::numeric(14,0) as avg_price
            FROM listings WHERE {where_sql} AND city IS NOT NULL
            GROUP BY city ORDER BY cnt DESC LIMIT 5
        """, params)
        top_cities = cur.fetchall()
        cur.execute(f"""
            SELECT property_type, COUNT(*) as cnt
            FROM listings WHERE {where_sql} AND property_type IS NOT NULL
            GROUP BY property_type ORDER BY cnt DESC
        """, params)
        by_type = cur.fetchall()

    return {
        "count":        int(row[0] or 0),
        "avg_price":    int(row[1] or 0),
        "min_price":    int(row[2] or 0),
        "max_price":    int(row[3] or 0),
        "median_price": int(row[4] or 0),
        "avg_price_m2": int(row[5] or 0),
        "avg_surface":  float(row[6] or 0),
        "avg_rooms":    float(row[7] or 0),
        "top_cities":   [{"city": r[0], "count": r[1], "avg_price": int(r[2] or 0)} for r in top_cities],
        "by_type":      [{"type": r[0], "count": r[1]} for r in by_type],
    }


def _format_period_text(period: dict) -> str:
    report_type = period.get("report_type", "monthly")
    start_date  = period.get("start_date")
    if not start_date:
        return "période la plus récente disponible"
    if report_type == "annual":
        return f"l'année {start_date[:4]}"
    if report_type == "quarterly":
        q = (int(start_date[5:7]) - 1) // 3 + 1
        return f"le T{q} {start_date[:4]}"
    if report_type == "monthly":
        months = ["janvier","février","mars","avril","mai","juin",
                  "juillet","août","septembre","octobre","novembre","décembre"]
        return f"{months[int(start_date[5:7])-1]} {start_date[:4]}"
    if report_type == "ytd":
        return f"du début {start_date[:4]} à aujourd'hui"
    end_date = period.get("end_date")
    return f"du {start_date} au {end_date}" if end_date else f"à partir du {start_date}"


# ── Report prompts ─────────────────────────────────────────────────────────────

def _market_prompt(params: dict, context: str, stats: dict) -> str:
    city      = params.get("city", "Tunisia")
    tx_type   = params.get("transaction_type", "sale and rent")
    period    = params.get("period", {})
    period_text = _format_period_text(period)
    return textwrap.dedent(f"""
        You are a senior real estate analyst specialising in the Tunisian property market.
        Write a professional Market Overview Report in English.

        ## Report Parameters
        - Geographic focus: {city if city else 'Tunisia (all regions)'}
        - Transaction type: {tx_type if tx_type else 'sale and rent'}
        - Analysis period: {period_text}
        - Report generated: {datetime.now().strftime('%B %d, %Y')}

        ## Live Market Data
        - Active listings analysed: {stats['count']:,}
        - Average price: {stats['avg_price']:,} TND
        - Median price: {stats['median_price']:,} TND
        - Price range: {stats['min_price']:,} – {stats['max_price']:,} TND
        - Average price per m²: {stats['avg_price_m2']:,} TND/m²
        - Average surface: {stats['avg_surface']:.0f} m²
        - Property type breakdown: {json.dumps(stats['by_type'])}

        ## Knowledge Base Context
        {context}

        ## Instructions
        Use EXACT numbers from the data above only. Do not invent statistics.
        Write sections:
        1. **Executive Summary** (3–4 sentences with key figures)
        2. **Market Overview** — current state, volume, demand drivers
        3. **Price Analysis** — prices, trends, by type and city
        4. **Supply & Demand Dynamics**
        5. **Investment Outlook** — rental yields, appreciation
        6. **Key Risks** — economic, regulatory, currency
        7. **Conclusion & Recommendations**
    """).strip()


def _investment_prompt(params: dict, context: str, stats: dict) -> str:
    city      = params.get("city", "")
    prop_type = params.get("property_type", "residential")
    budget_min = params.get("budget_min", 0)
    budget_max = params.get("budget_max", 5_000_000)
    tx_type   = params.get("transaction_type", "sale")
    period    = params.get("period", {})
    period_text = _format_period_text(period)
    city_lines = "\n".join(
        f"  - {r['city']}: {r['count']} listings, avg {r['avg_price']:,} TND"
        for r in stats["top_cities"]
    ) if stats["top_cities"] else "  (no city filter applied)"
    return textwrap.dedent(f"""
        You are a real estate investment advisor specialising in Tunisia.
        Write a professional Investment Analysis Report in English.

        ## Investment Parameters
        - Target: {city or 'Tunisia (all regions)'}
        - Property type: {prop_type if prop_type else 'all types'}
        - Transaction: {tx_type}
        - Budget: {budget_min:,} – {budget_max:,} TND
        - Analysis period: {period_text}
        - Report generated: {datetime.now().strftime('%B %d, %Y')}

        ## Market Data
        - Matching listings: {stats['count']:,}
        - Average price: {stats['avg_price']:,} TND
        - Median price: {stats['median_price']:,} TND
        - Average price/m²: {stats['avg_price_m2']:,} TND/m²
        - Average surface: {stats['avg_surface']:.0f} m²
        - Top markets:
        {city_lines}

        ## Knowledge Base Context
        {context}

        ## Instructions
        Use EXACT figures from data. Be direct, make a clear recommendation.
        Write sections:
        1. **Investment Summary** — direct answer: good investment or not?
        2. **Market Position**
        3. **Financial Analysis** with specific numbers
        4. **Risk Assessment**
        5. **Strategic Recommendations** — buy / wait / avoid
    """).strip()


# ══════════════════════════════════════════════════════════════════════════════
# CONTRACT TEMPLATES — VERBATIM FROM OFFICIAL DOCUMENTS
# Structure is FIXED. Only {placeholders} are filled at runtime.
# ══════════════════════════════════════════════════════════════════════════════

_TEMPLATE_PROMESSE_DE_VENTE = """\
PROMESSE DE VENTE

Entre les soussignés,

1. {seller_name}, de nationalité tunisienne, né(e) le {seller_dob} à {seller_place_birth},
   demeurant au {seller_address}, titulaire de la CIN n° {seller_cin},
   ci-après désigné(e) « le Promettant »,

2. {buyer_name}, de nationalité tunisienne, né(e) le {buyer_dob} à {buyer_place_birth},
   demeurant au {buyer_address}, titulaire de la CIN n° {buyer_cin},
   ci-après désigné(e) « le Bénéficiaire »,

Il est tout d'abord exposé ce qui suit :

Le Promettant est propriétaire du bien immobilier objet de la présente promesse,
régi par la loi n°17 du 26 février 1990 relative à la promotion immobilière
et le décret n°1330 du 28 août 1991 relatif au cahier des charges organisant
la profession de promoteur immobilier.

Ceci étant exposé, il a été arrêté et convenu ce qui suit :

Article 1 : Promesse de Vente
Le Promettant promet de vendre sous toutes les garanties de fait et de droit
au Bénéficiaire, qui accepte :
{listing_title}, situé au {listing_address},
d'une superficie couverte totale approximative de {surface} m²
y compris la surface des parties communes.

Article 2 : Documents et Prestations offertes
Le Bénéficiaire reconnaît avoir visité le bien objet de la présente promesse
et avoir examiné une copie de plan qui lui a été remise à la signature des présentes.

Article 3 : Prix et modalité de paiement
Le prix du bien objet des présentes est fixé à la somme de
{price_letters} Dinars ({price} DT).
Ce prix est ferme et non révisable, et est payable de la manière suivante :
- {deposit} DT à la signature des présentes ;
- Le reliquat par crédit bancaire auprès de l'établissement financier du Bénéficiaire ;
- Le solde restant à la signature de l'acte final.
Des reçus de paiement seront octroyés au Bénéficiaire à chaque paiement.

Article 4 : Transcription de la vente
Le Bénéficiaire s'engage à signer tous les actes de précisions/additifs nécessaires
pour la transcription de la vente auprès de la Conservation de la Propriété Foncière
et à la distraction du titre foncier individuel.

Article 5 : Distraction de titre
En application de l'article 16 du décret n° 1330 du 28 Août 1991, le Promettant
s'oblige de procéder aux opérations de transcription des actes de vente auprès
de la Conservation de la Propriété Foncière et de la distraction du titre foncier
pour chaque local vendu.
Le Bénéficiaire se chargera, suite à ces opérations, des procédures de mutation
du titre foncier individuel en son nom personnel.

Article 6 : Date de remise des clefs
La date de remise des clefs est prévue pour le {delivery_date}.
Un délai supplémentaire d'un mois peut être ajouté à cette date en cas de force majeure.

Article 7 : Pénalité de retard
Conformément à l'article 15 du même décret ci-haut mentionné, le Promettant encourt
une pénalité de retard fixée à 1/2000ème du total des avances payées par jour de retard
par rapport au délai contractuel, sans pour autant que la pénalité puisse dépasser
15% du total desdites avances.
La pénalité commence à courir à partir de la date d'une mise en demeure adressée
au Promettant par exploit d'huissier-notaire.

Article 8 : Prise de possession
La prise de possession du local et son occupation par le Bénéficiaire s'effectuera
après l'achèvement de l'ensemble de l'immeuble, de l'aménagement des parties communes
et l'obtention du permis d'occuper.
Dès lors, le Bénéficiaire complétera le solde du prix et les frais indiqués ci-dessous
et signera le contrat de vente.
La date de la remise des clés sera fixée par le Promettant dans les limites des délais
d'exécution prévus à l'article 6.

Article 9 : Modifications
Le Bénéficiaire ne pourra apporter aucune modification au local sur les façades
extérieures tant avant qu'après la mise en possession sans autorisation écrite
et conjointe du Promettant et/ou du syndic de l'immeuble une fois le syndicat constitué.
Il ne peut non plus amener une quelconque modification de la distribution interne
de nature à porter une quelconque nuisance ou atteinte à un mur mitoyen ou ouvrage
de solidité.

Article 10 : Règlement de copropriété
Le Bénéficiaire adhère sans réserve au règlement de copropriété à établir par
le Promettant. Il accepte d'avance le partage des parties communes.
Le dit règlement demeurera en vigueur jusqu'à l'établissement par le Promettant
d'un nouveau règlement ou d'un complément au premier suite au lotissement de l'immeuble.

Article 11 : Charges
Le Bénéficiaire aura obligatoirement à sa charge avant la signature du contrat de vente :
1) Les frais et honoraires relatifs aux opérations de distraction du titre foncier individuel ;
2) Les frais de rédaction de la promesse et du contrat de vente ainsi que l'acte de précision ;
3) Les frais de syndic pendant la première année à partir de la date de remise des clefs ;
4) Les frais de branchement de l'électricité, de l'eau courante et du gaz de ville
   (compteurs et installation compris) ainsi que les frais de l'avance sur consommation.
À partir de la date de signature du contrat de vente, le Bénéficiaire acquittera
proportionnellement à sa part, tout impôt, taxe et contribution quelconques
grevant le local ou pouvant le grever.

Article 12 : Résiliation
La présente promesse sera résiliée de plein droit en cas de :
- non-respect d'une des clauses du présent contrat, et particulièrement ;
- non-paiement par le Bénéficiaire du solde du prix ou des frais dans les délais impartis ;
- non-obtention d'un crédit bancaire pour le paiement du reliquat du prix de vente.
Dans ce cas, le Promettant peut, après une mise en demeure par exploit d'huissier-notaire
demeurée sans effet dans un délai d'un mois après son envoi, consigner à la Trésorerie
Générale de Tunisie les avances consenties par le Bénéficiaire après déduction de 10%
pour frais de dédommagement.
Aussitôt après la consignation, le Promettant peut disposer du local et le vendre
à une tierce personne.
En cas de résiliation par le Bénéficiaire, il sera fait application de l'article 17
de la loi n° 17 du 26 février 1990 relative à la promotion immobilière.

Article 13 : Crédit
Il est formellement précisé que pour les clients bénéficiaires d'un crédit bancaire
ou autre, l'accord audit crédit doit être présenté au Promettant au plus tard
3 mois avant la date de l'achèvement ci-haut fixée, faute de quoi la présente
promesse sera résiliée suivant les formes et conditions de résiliation fixées
à l'article précédent.
Si la promesse de vente est effectuée après l'achèvement des travaux et l'obtention
du permis d'occupation, ce délai est réduit à 1 mois.

Article 14 : Attribution de compétence
Pour l'interprétation et l'exécution des clauses de la présente promesse, seuls
les tribunaux du gouvernorat de {jurisdiction} sont compétents.

Article 15 : Frais
Les frais de rédaction, de timbre et d'enregistrement des présentes, du contrat
de vente ou de tout autre acte ou avenant y afférent sont à la charge du Bénéficiaire,
qui s'y oblige.

Fait à {city}, le {transaction_date}, en deux exemplaires originaux.

Le rédacteur de l'acte


Le Promettant                                         Le Bénéficiaire
{seller_name}                                         {buyer_name}
"""


_TEMPLATE_CONTRAT_DE_LOCATION = """\
CONTRAT TYPE DE LOCATION DE LOGEMENT MEUBLÉ
(Soumis au titre Ier bis de la loi du 6 juillet 1989 tendant à améliorer les rapports
locatifs et portant modification de la loi n° 86-1290 du 23 décembre 1986)

Le présent contrat est conclu entre les soussignés :

I. Désignation des parties

Nom et prénom du bailleur : {seller_name}
Qualité du bailleur : Personne physique
Adresse : {seller_address}
CIN n° : {seller_cin}
désigné(s) ci-après « le bailleur » ;

Nom et prénom du locataire : {buyer_name}
Adresse : {buyer_address}
CIN n° : {buyer_cin}
désigné(s) ci-après « le locataire » ;

Il a été convenu ce qui suit :

II. Objet du contrat

II.1. Consistance du logement
Adresse du logement : {listing_address}
Désignation : {listing_title}
Surface habitable : {surface} m²
Destination des locaux : usage d'habitation (logement meublé)

II.2. Désignation des locaux et équipements accessoires à usage privatif du locataire :
Logement meublé comprenant : mobilier de séjour, literie, cuisine équipée,
appareils électroménagers essentiels, équipements sanitaires complets.

II.3. Le cas échéant, énumération des locaux, parties, équipements et accessoires
de l'immeuble à usage commun : selon état des lieux d'entrée annexé.

III. Date de prise d'effet et durée du contrat

Date de prise d'effet du contrat : {transaction_date}
Durée du contrat : 1 (une) année — durée réduite conformément à la loi du 6 juillet 1989.
En l'absence de proposition de renouvellement, le contrat est reconduit tacitement
dans les mêmes conditions.
Le locataire peut mettre fin au bail à tout moment, après avoir donné congé.
Le bailleur peut mettre fin au bail à son échéance et après avoir donné congé,
soit pour reprendre le logement, soit pour le vendre, soit pour motif sérieux et légitime.

IV. Conditions financières

IV.1. Loyer — Fixation du loyer initial
Montant du loyer mensuel : {monthly_rent} DT (Dinars Tunisiens)

IV.2. Loyer — Modalités de révision
Le loyer sera révisé annuellement à la date anniversaire du contrat,
dans la limite de l'évolution de l'indice des prix à la consommation (IPC)
publié par l'Institut National de la Statistique (INS) de Tunisie.

IV.3. Charges récupérables
Modalité de règlement des charges récupérables :
Provisions sur charges avec régularisation annuelle.
Montant des provisions sur charges : {charges} DT par mois.

IV.6. Modalités de paiement
Périodicité du paiement : mensuel, d'avance, le premier de chaque mois.
Lieu de paiement : au domicile du bailleur ou par virement bancaire.

V. Travaux
Le locataire ne pourra effectuer aucuns travaux de transformation sans accord
écrit et préalable du bailleur. Les améliorations réalisées sans accord
resteront acquises au bailleur sans indemnité.

VI. Garanties
Montant du dépôt de garantie (inférieur ou égal à deux mois de loyers hors charges) :
{deposit} DT.
Ce dépôt sera restitué dans un délai d'un mois suivant la remise des clefs,
déduction faite des sommes éventuellement dues au bailleur.

VIII. Clause résolutoire
Le bail sera résilié de plein droit en cas d'inexécution des obligations du locataire,
soit en cas de :
- défaut de paiement des loyers et charges locatives au terme convenu ;
- non-versement du dépôt de garantie ;
- défaut d'assurance du locataire contre les risques locatifs ;
- troubles de voisinage constatés par décision de justice passée en force de chose jugée.
Le bailleur devra préalablement signifier au locataire, par acte d'huissier,
un commandement de payer. Si le locataire ne s'est pas acquitté dans les deux mois
suivant la signification, le bailleur peut assigner le locataire en justice.

X. Autres conditions particulières
Toute sous-location totale ou partielle est interdite sans accord écrit préalable du bailleur.
Le locataire s'engage à entretenir le logement en bon état et à en user paisiblement.
Le locataire est tenu de justifier d'une assurance multirisques habitation
couvrant les risques locatifs dès la signature du présent contrat.

XI. Annexes
Sont annexés et joints au présent contrat :
a. Un état des lieux d'entrée contradictoire, signé par les deux parties ;
b. Un inventaire détaillé du mobilier et des équipements ;
c. L'attestation d'assurance habitation du locataire.

XII. Signatures

Fait à {city}, le {transaction_date}, en deux exemplaires originaux,
dont un remis ce jour au locataire qui le reconnaît.

Signature du bailleur                               Signature du locataire
{seller_name}                                       {buyer_name}
"""


_TEMPLATE_COMPROMIS_DE_VENTE = """\
COMPROMIS DE VENTE

Entre les soussignés :

Vendeur : {seller_name}, de nationalité tunisienne, né(e) le {seller_dob},
demeurant au {seller_address}, titulaire de la CIN n° {seller_cin},
ci-après désigné « le Vendeur »,

Acquéreur : {buyer_name}, de nationalité tunisienne, né(e) le {buyer_dob},
demeurant au {buyer_address}, titulaire de la CIN n° {buyer_cin},
ci-après désigné « l'Acquéreur »,

Il a été convenu ce qui suit :

Article 1 : Désignation du bien
Le Vendeur vend à l'Acquéreur, qui accepte d'acquérir :
{listing_title}, situé au {listing_address}, d'une superficie de {surface} m².

Article 2 : Prix
Le prix de vente est fixé à {price_letters} Dinars ({price} DT).
Un acompte de {deposit} DT est versé à la signature des présentes.
Le solde sera réglé à la signature de l'acte authentique.

Article 3 : Conditions suspensives
La présente vente est conclue sous les conditions suspensives suivantes :
- Obtention par l'Acquéreur d'un prêt bancaire d'un montant de {loan_amount} DT
  dans un délai de 3 mois à compter de la signature des présentes ;
- Obtention de toutes les autorisations administratives requises.
À défaut de réalisation dans le délai imparti, le présent compromis sera caduc
de plein droit et l'acompte restitué intégralement à l'Acquéreur.

Article 4 : Délai de rétractation
L'Acquéreur bénéficie d'un délai de rétractation de 10 (dix) jours
à compter de la première présentation de la lettre lui notifiant les présentes.

Article 5 : Clause pénale
En cas de rétractation de l'Acquéreur hors délai légal ou de refus d'exécuter la vente,
l'acompte de {deposit} DT restera acquis au Vendeur à titre de clause pénale.
En cas de refus de vente par le Vendeur, celui-ci devra restituer le double
de l'acompte reçu, soit {double_deposit} DT.

Article 6 : Date de signature de l'acte authentique
Les parties s'engagent à signer l'acte authentique de vente au plus tard le {final_date},
les frais étant à la charge exclusive de l'Acquéreur.

Article 7 : Droits et frais
Les droits d'enregistrement (3% du prix) et les frais de notaire
sont à la charge exclusive de l'Acquéreur.

Article 8 : Attribution de compétence
Les tribunaux du gouvernorat de {jurisdiction} sont seuls compétents.

Fait à {city}, le {transaction_date}, en deux exemplaires originaux.

Le Vendeur                                            L'Acquéreur
{seller_name}                                         {buyer_name}
"""


_TEMPLATE_ACTE_DE_VENTE = """\
ACTE DE VENTE

Entre les soussignés :

VENDEUR :
{seller_name}, de nationalité tunisienne, né(e) le {seller_dob} à {seller_place_birth},
demeurant au {seller_address}, titulaire de la CIN n° {seller_cin},
ci-après désigné « le Vendeur »,

ACQUÉREUR :
{buyer_name}, de nationalité tunisienne, né(e) le {buyer_dob} à {buyer_place_birth},
demeurant au {buyer_address}, titulaire de la CIN n° {buyer_cin},
ci-après désigné « l'Acquéreur »,

ORIGINE DE PROPRIÉTÉ
Le Vendeur est propriétaire du bien ci-après désigné en vertu de {origin_of_title}.

DÉSIGNATION DU BIEN
{listing_title}, situé au {listing_address}, d'une superficie de {surface} m²,
figurant au titre foncier n° {land_title_number}.

PRIX ET CONDITIONS
Le présent acte de vente est consenti et accepté moyennant le prix principal
de {price_letters} Dinars ({price} DT), dont quittance est donnée par le Vendeur,
qui reconnaît avoir reçu ladite somme en totalité avant la signature des présentes.

QUITTANCE DU PRIX
Le Vendeur donne quittance de la totalité du prix ci-dessus, sans réserve ni condition.

ATTRIBUTION DE JOUISSANCE
L'Acquéreur entre en jouissance du bien vendu à compter de ce jour.
Le Vendeur lui remet en ce moment les clefs et tous documents relatifs au bien.

GARANTIES LÉGALES
Le Vendeur garantit l'Acquéreur contre tout trouble de droit ou de fait (garantie d'éviction)
et contre tout vice caché (garantie des vices cachés), conformément au Code des
Obligations et des Contrats tunisien.

CHARGES ET CONDITIONS
Le présent acte est consenti sous les charges et conditions ordinaires et de droit,
notamment les servitudes actives et passives, apparentes et non apparentes,
continues et discontinues pouvant grever le bien.

FRAIS ET HONORAIRES
Tous les frais, droits d'enregistrement (3% du prix), honoraires de notaire
et frais de publication foncière sont à la charge exclusive de l'Acquéreur.

ÉLECTION DE DOMICILE
Pour l'exécution des présentes, les parties font élection de domicile
en leur demeure respective telle qu'indiquée ci-dessus.

FORMALITÉS DE PUBLICATION
Le présent acte sera soumis aux formalités de publication à la Conservation
de la Propriété Foncière de {jurisdiction}, conformément au Code des Droits Réels.

Fait à {city}, le {transaction_date}, en deux exemplaires originaux.

Le Vendeur                                            L'Acquéreur
{seller_name}                                         {buyer_name}
"""


_TEMPLATES = {
    "promesse_de_vente":   _TEMPLATE_PROMESSE_DE_VENTE,
    "contrat_de_location": _TEMPLATE_CONTRAT_DE_LOCATION,
    "compromis_de_vente":  _TEMPLATE_COMPROMIS_DE_VENTE,
    "acte_de_vente":       _TEMPLATE_ACTE_DE_VENTE,
}


def _fill_contract(params: dict, contract_type: str) -> str:
    """Fill template with party/property data. Missing fields → '_____'."""
    p = params
    try:
        price = int(float(str(p.get("price", 0)).replace(",", "")))
    except (ValueError, TypeError):
        price = 0
    try:
        monthly_rent = int(float(str(p.get("monthly_rent", 0)).replace(",", "")))
    except (ValueError, TypeError):
        monthly_rent = 0

    deposit = round(price * 0.10) if price > 0 else 0
    try:
        deposit = int(float(str(p.get("deposit", deposit)).replace(",", "")))
    except (ValueError, TypeError):
        pass
    double_deposit = deposit * 2

    template = _TEMPLATES.get(contract_type, _TEMPLATE_PROMESSE_DE_VENTE)
    return template.format(
        seller_name        = p.get("seller_name", "_____"),
        seller_cin         = p.get("seller_cin", "_____"),
        seller_address     = p.get("seller_address", "_____"),
        seller_dob         = p.get("seller_dob", "_____"),
        seller_place_birth = p.get("seller_place_birth", "_____"),
        buyer_name         = p.get("buyer_name", "_____"),
        buyer_cin          = p.get("buyer_cin", "_____"),
        buyer_address      = p.get("buyer_address", "_____"),
        buyer_dob          = p.get("buyer_dob", "_____"),
        buyer_place_birth  = p.get("buyer_place_birth", "_____"),
        listing_title      = p.get("listing_title", "bien immobilier"),
        listing_address    = p.get("listing_address", p.get("city", "_____")),
        surface            = p.get("surface", "_____"),
        price              = f"{price:,}" if price else "_____",
        price_letters      = p.get("price_letters", "_____"),
        deposit            = f"{deposit:,}" if deposit else "_____",
        double_deposit     = f"{double_deposit:,}" if double_deposit else "_____",
        monthly_rent       = f"{monthly_rent:,}" if monthly_rent else "_____",
        charges            = p.get("charges", "_____"),
        transaction_date   = p.get("transaction_date", "_____"),
        delivery_date      = p.get("delivery_date", "_____"),
        final_date         = p.get("final_date", "_____"),
        loan_amount        = p.get("loan_amount", "_____"),
        origin_of_title    = p.get("origin_of_title", "acte de vente antérieur"),
        land_title_number  = p.get("land_title_number", "_____"),
        jurisdiction       = p.get("jurisdiction", p.get("city", "Tunis")),
        city               = p.get("city", "Tunis"),
    )


def _contract_prompt(params: dict, contract_type: str) -> str:
    filled = _fill_contract(params, contract_type)
    return textwrap.dedent(f"""
        Tu es un notaire tunisien. Le contrat ci-dessous est déjà rédigé et structuré
        selon le modèle officiel. Il contient des champs "_____" pour les informations
        non fournies.

        INSTRUCTIONS STRICTES — À RESPECTER ABSOLUMENT :
        1. RECOPIE le contrat EXACTEMENT tel quel, mot pour mot, article par article.
        2. Pour chaque champ "_____", remplace uniquement par la formule juridique
           standard appropriée (ex: "selon modalités convenues", "à déterminer").
        3. NE CHANGE PAS, NE SUPPRIME PAS, NE MODIFIE PAS la moindre clause ou article.
        4. NE RAJOUTE AUCUN article, commentaire ou explication.
        5. Retourne UNIQUEMENT le contrat final, rien d'autre.

        CONTRAT À FINALISER :

        {filled}
    """).strip()


# ── Ollama streaming ───────────────────────────────────────────────────────────

def _stream_ollama(prompt: str, temperature: float = 0.3) -> Generator[str, None, None]:
    import httpx
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are EstateMind's AI. For reports: be precise and data-driven. "
                    "For contracts: copy the provided template exactly, only fill blanks."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "stream": True,
        "options": {
            "temperature": temperature,
            "num_predict": MAX_TOKENS,
            "num_ctx":     8192,
        },
    }
    with httpx.stream(
        "POST", f"{OLLAMA_BASE_URL}/api/chat",
        json=payload, timeout=300,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            try:
                data  = json.loads(line)
                token = data.get("message", {}).get("content", "")
                if token:
                    yield token
                if data.get("done"):
                    break
            except json.JSONDecodeError:
                continue


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_report_stream(report_type: str, params: dict) -> Generator[str, None, None]:
    """Generate market or investment report. Temperature = 0.3."""
    period = params.get("period", {})
    if report_type == "market":
        query   = f"real estate market Tunisia {params.get('city', '')} {params.get('transaction_type', '')}"
        chunks  = retrieve_context(query, TOP_K)
        context = _format_context(chunks)
        stats   = _get_listing_stats({
            "city": params.get("city"), "transaction_type": params.get("transaction_type"), "period": period,
        })
        prompt = _market_prompt(params, context, stats)
    elif report_type == "investment":
        query   = f"investment Tunisia {params.get('city', '')} {params.get('property_type', '')}"
        chunks  = retrieve_context(query, TOP_K)
        context = _format_context(chunks)
        stats   = _get_listing_stats({
            "city": params.get("city"), "region": params.get("region"),
            "transaction_type": params.get("transaction_type", "sale"),
            "property_type": params.get("property_type"), "period": period,
        })
        prompt = _investment_prompt(params, context, stats)
    else:
        yield f"Unknown report type: {report_type}"
        return
    yield from _stream_ollama(prompt, temperature=0.3)


def generate_contract_stream(contract_type: str, params: dict) -> Generator[str, None, None]:
    """
    Generate contract from official template. Temperature = 0.0.
    Fallback: yields the pre-filled template directly if Ollama fails.
    """
    valid = ["promesse_de_vente", "compromis_de_vente", "contrat_de_location", "acte_de_vente"]
    if contract_type not in valid:
        yield f"Erreur : type de contrat inconnu '{contract_type}'"
        return
    try:
        prompt = _contract_prompt(params, contract_type)
    except Exception as e:
        yield f"Erreur lors de la préparation du contrat : {e}"
        return
    try:
        yield from _stream_ollama(prompt, temperature=0.0)
    except Exception as e:
        yield f"[Note: LLM indisponible ({e}). Contrat pré-rempli ci-dessous]\n\n"
        yield _fill_contract(params, contract_type)