import pandas as pd

INPUT = "dns_full_results.csv"
OUTPUT_XLSX = "dns_results_cleaned_classified.xlsx"
OUTPUT_CSV = "dns_results_cleaned_classified.csv"

df = pd.read_csv(INPUT)

# 1. Remove non-domain junk rows
df = df[df["domain"].astype(str).str.contains(r"\.", na=False)].copy()

# 2. Reachability classification
def classify_reachability(row):
    if row["status"] == "OK" and pd.notna(row["ipv4"]):
        return "Public (IPv4)"
    if row["status"] == "OK" and pd.isna(row["ipv4"]) and pd.notna(row["ipv6"]):
        return "Public (IPv6 only)"
    if row["status"] == "NXDOMAIN":
        return "No Public DNS / Internal / Legacy"
    if "timeout" in str(row["ipv4"]).lower():
        return "DNS Resolver Timeout"
    return "Unknown"

# 3. Hosting classification
def classify_hosting(row):
    cname = str(row["cname"]).lower()
    if "cloudflare" in cname:
        return "CDN (Cloudflare)"
    if "azure" in cname or "azurewebsites" in cname:
        return "Cloud (Azure)"
    if "imperva" in cname:
        return "WAF/CDN (Imperva)"
    if "webflow" in cname:
        return "CDN (Webflow)"
    if pd.notna(row["ipv4"]):
        return "Direct / Shared Hosting"
    return "Unknown"

df["Reachability Classification"] = df.apply(classify_reachability, axis=1)
df["Hosting Classification"] = df.apply(classify_hosting, axis=1)

# 4. Shared IP analysis
ip_counts = (
    df.dropna(subset=["ipv4"])
      .groupby("ipv4")["domain"]
      .count()
      .reset_index(name="Domains on IP")
)

df = df.merge(ip_counts, on="ipv4", how="left")
df["Domains on IP"] = df["Domains on IP"].fillna(0).astype(int)

# 5. Save outputs
df.to_csv(OUTPUT_CSV, index=False)
df.to_excel(OUTPUT_XLSX, index=False)

print("✔ Cleaned & classified DNS results saved")
print("✔ Files:", OUTPUT_CSV, OUTPUT_XLSX)
