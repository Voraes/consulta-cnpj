import requests

def consulta_cnpj(cnpj):
    url = f"https://open.cnpja.com/office/{cnpj}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def regime_tributario(cnpj):
    data = consulta_cnpj(cnpj)

    return {
        "cnpj": cnpj,
        "simples_nacional": data["company"]["simples"]["optant"],
        "mei": data["company"]["simei"]["optant"]
    }

if __name__ == "__main__":
    print(regime_tributario("07526557011659"))
