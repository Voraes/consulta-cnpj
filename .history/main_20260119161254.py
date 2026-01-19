import requests
import time
import sys
import json
import os
from datetime import datetime, timedelta

API_URL = "https://open.cnpja.com/office"
RATE_LIMIT_SECONDS = 12  # 5 req/min
TIMEOUT = 15
RETRY_BACKOFF = 30  # segundos
CACHE_FILE = "cnpj_cache.json"
CACHE_TTL_DAYS = 30


# ________Utils________

def normalizar_cnpj(cnpj):
    return "".join(filter(str.isdigit, cnpj))


def validar_cnpj(cnpj):
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    def calc_dv(cnpj, peso):
        soma = sum(int(d) * p for d, p in zip(cnpj, peso))
        resto = soma % 11
        return "0" if resto < 2 else str(11 - resto)

    peso1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    peso2 = [6] + peso1

    dv1 = calc_dv(cnpj[:12], peso1)
    dv2 = calc_dv(cnpj[:12] + dv1, peso2)

    return cnpj[-2:] == dv1 + dv2


def carregar_cache():
    if not os.path.exists(CACHE_FILE):
        return {}

    with open(CACHE_FILE, "r") as f:
        return json.load(f)


def salvar_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def cache_valido(entry):
    atualizado = datetime.fromisoformat(entry["updated_at"])
    return datetime.utcnow() - atualizado < timedelta(days=CACHE_TTL_DAYS)


# ---------------------------
# Consulta
# ---------------------------

def consulta_api(cnpj):
    try:
        r = requests.get(
            f"{API_URL}/{cnpj}",
            timeout=TIMEOUT
        )

        if r.status_code == 404:
            return {"status": "NAO_ENCONTRADO"}

        if r.status_code == 429:
            return {"status": "RATE_LIMIT"}

        r.raise_for_status()
        data = r.json()

        return {
            "status": "OK",
            "simples_nacional": data["company"]["simples"]["optant"],
            "mei": data["company"]["simei"]["optant"]
        }

    except requests.exceptions.Timeout:
        return {"status": "TIMEOUT"}
    except requests.exceptions.RequestException as e:
        return {"status": "ERRO", "erro": str(e)}


# ---------------------------
# Pipeline principal
# ---------------------------

def processar_lote(cnpjs):
    cache = carregar_cache()
    resultados = []

    for i, bruto in enumerate(cnpjs):
        cnpj = normalizar_cnpj(bruto)

        if not validar_cnpj(cnpj):
            resultados.append({
                "cnpj": bruto,
                "status": "INVALIDO"
            })
            continue

        # Cache
        if cnpj in cache and cache_valido(cache[cnpj]):
            resultados.append(cache[cnpj]["resultado"])
            continue

        # Consulta API
        resultado = consulta_api(cnpj)

        # Retry único
        if resultado["status"] in {"TIMEOUT", "ERRO"}:
            time.sleep(RETRY_BACKOFF)
            resultado = consulta_api(cnpj)

        resposta_final = {
            "cnpj": cnpj,
            **resultado
        }

        # Salva cache apenas se resposta útil
        if resultado["status"] == "OK":
            cache[cnpj] = {
                "updated_at": datetime.utcnow().isoformat(),
                "resultado": resposta_final
            }

        resultados.append(resposta_final)

        # Rate limit
        if i < len(cnpjs) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    salvar_cache(cache)
    return resultados


# ---------------------------
# Main
# ---------------------------

if __name__ == "__main__":
    """
    Uso:
    echo '["07.526.557/0116-59", "12345678000199"]' | python3 consulta_cnpj_lote.py
    """

    entrada = json.load(sys.stdin)
    saida = processar_lote(entrada)

    print(json.dumps(saida, ensure_ascii=False, indent=2))
