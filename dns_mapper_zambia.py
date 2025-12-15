import pandas as pd
import socket
import datetime

# ========= CONFIG =========
INPUT_FILE = "zambia_gov_entities_comprehensive.xlsx"
OUTPUT_FILE = "zambia_gov_entities_with_dns.xlsx"

DOMAIN_COL = "Primary Domain"
ALT_DOMAIN_COL = "Subdomain/Alternate"
IP_COL = "IP Address"
# ==========================

df = pd.read_excel(INPUT_FILE)

# Add output columns if missing
output_columns = [
    "Resolved IPv4",
    "Resolved IPv6",
    "CNAME",
    "DNS Status",
    "Resolution Source",
    "Last Checked"
]

for col in output_columns:
    if col not in df.columns:
        df[col] = ""

def resolve_dns(domain):
    result = {
        "ipv4": None,
        "ipv6": None,
        "cname": None,
        "status": "OK"
    }

    try:
        # IPv4 (A record)
        try:
            result["ipv4"] = socket.gethostbyname(domain)
        except:
            result["ipv4"] = None

        # IPv6 (AAAA record)
        try:
            infos = socket.getaddrinfo(domain, None, socket.AF_INET6)
            if infos:
                result["ipv6"] = infos[0][4][0]
        except:
            result["ipv6"] = None

        # CNAME
        try:
            cname = socket.gethostbyname_ex(domain)[0]
            if cname != domain:
                result["cname"] = cname
        except:
            result["cname"] = None

        if not result["ipv4"] and not result["ipv6"]:
            result["status"] = "No public DNS records"

    except Exception as e:
        result["status"] = f"ERROR: {str(e)}"

    return result

for idx, row in df.iterrows():
    domain = str(row.get(DOMAIN_COL, "")).strip().lower()
    alt_domain = str(row.get(ALT_DOMAIN_COL, "")).strip().lower()

    # Decide which domain to query
    query_domain = None
    source = None

    if domain not in ["", "nan", "none", "false"]:
        query_domain = domain
        source = "Primary Domain"
    elif alt_domain not in ["", "nan", "none", "false"]:
        query_domain = alt_domain
        source = "Subdomain/Alternate"
    else:
        df.at[idx, "DNS Status"] = "No domain available"
        continue

    dns = resolve_dns(query_domain)

    df.at[idx, "Resolved IPv4"] = dns["ipv4"]
    df.at[idx, "Resolved IPv6"] = dns["ipv6"]
    df.at[idx, "CNAME"] = dns["cname"]
    df.at[idx, "DNS Status"] = dns["status"]
    df.at[idx, "Resolution Source"] = source
    df.at[idx, "Last Checked"] = datetime.date.today().isoformat()

    # 🔑 Write IPv4 into your EXISTING IP Address column
    if dns["ipv4"]:
        df.at[idx, IP_COL] = dns["ipv4"]

df.to_excel(OUTPUT_FILE, index=False)

print("✔ DNS mapping complete")
print("✔ Output written to:", OUTPUT_FILE)
