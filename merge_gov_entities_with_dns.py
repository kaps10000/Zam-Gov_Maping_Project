import pandas as pd

# Load files
gov = pd.read_excel("zambia_gov_entities_comprehensive.xlsx")
dns = pd.read_excel("dns_results_cleaned_classified.xlsx")

# --- Normalization function ---
def clean_domain(x):
    if pd.isna(x):
        return ""
    x = str(x).lower().strip()
    x = x.replace("http://", "").replace("https://", "")
    return x.rstrip("/")

# Normalize domains
gov["Primary Domain Clean"] = gov["Primary Domain"].apply(clean_domain)
gov["Subdomain Clean"] = gov["Subdomain/Alternate"].apply(clean_domain)
dns["domain_clean"] = dns["domain"].apply(clean_domain)

# DNS lookup dictionary
dns_map = dns.set_index("domain_clean").to_dict("index")

# --- Helper to extract DNS info ---
def lookup(domain):
    if domain in dns_map:
        return dns_map[domain]
    return {"ipv4": "", "ipv6": "", "cname": "", "status": "NOT_FOUND"}

# --- Apply lookups ---
primary_results = gov["Primary Domain Clean"].apply(lookup)
sub_results = gov["Subdomain Clean"].apply(lookup)

# Expand Primary
gov["Primary_IPv4"] = primary_results.apply(lambda x: x.get("ipv4", ""))
gov["Primary_IPv6"] = primary_results.apply(lambda x: x.get("ipv6", ""))
gov["Primary_CNAME"] = primary_results.apply(lambda x: x.get("cname", ""))
gov["Primary_DNS_Status"] = primary_results.apply(lambda x: x.get("status", ""))

# Expand Subdomain
gov["Subdomain_IPv4"] = sub_results.apply(lambda x: x.get("ipv4", ""))
gov["Subdomain_IPv6"] = sub_results.apply(lambda x: x.get("ipv6", ""))
gov["Subdomain_CNAME"] = sub_results.apply(lambda x: x.get("cname", ""))
gov["Subdomain_DNS_Status"] = sub_results.apply(lambda x: x.get("status", ""))

# Drop helper columns
gov.drop(columns=["Primary Domain Clean", "Subdomain Clean"], inplace=True)

# Save final merged file
gov.to_excel("zambian_gov_comprehensive_WITH_IPS.xlsx", index=False)

print("Merge complete → zambian_gov_comprehensive_WITH_IPS.xlsx")
