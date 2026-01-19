import requests
import time
import sys
import json

API_URL = "https://open.cnpja.com/office"
RATE_LIMIT_SECONDS = 12  # 5 req/min

def consulta_cnpj(cnpj):
    try:
        r = requests.get(f"{API_URL}/{cnpj}", timeout=15)

        if r.status_code == 404:
            return {
                "cnpj": cnpj,
                "status": "NAO_ENCONTRADO"
            }

        r.raise_for_status()
        data = r.json()

        return {
            "cnpj": cnpj,
            "status": "OK",
            "simples_nacional": data["company"]["simples"]["optant"],
            "mei": data["company"]["simei"]["optant"]
        }

    except requests.exceptions.Timeout:
        return {
            "cnpj": cnpj,
            "status": "TIMEOUT"
        }
    except requests.exceptions.RequestException as e:
        return {
            "cnpj": cnpj,
            "status": "ERRO",
            "erro": str(e)
        }

def processar_lote(cnpjs):
    resultados = []

    for i, cnpj in enumerate(cnpjs):
        resultado = consulta_cnpj(cnpj)
        resultados.append(resultado)

        # evita sleep depois da última requisição
        if i < len(cnpjs) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    return resultados

if __name__ == "__main__":
    # Entrada via stdin (ideal para Ansible)
    # Exemplo:
    # echo '["07526557011659","12345678000199"]' | python3 consulta_cnpj_lote.py

    cnpjs = json.load(sys.stdin)
    resultados = processar_lote(cnpjs)

    print(json.dumps(resultados, ensure_ascii=False, indent=2))
