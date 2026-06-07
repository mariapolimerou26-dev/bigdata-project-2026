import json
import matplotlib.pyplot as plt
import numpy as np

# 1. Φόρτωση των δεδομένων από το JSON
with open("eda_metrics.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# ==========================================
# ΔΙΑΓΡΑΜΜΑ 1: Κατανομή Ωρών (2015 vs 2024)
# ==========================================
hours_15 = sorted([int(k) for k in data["hourly_distribution_2015"].keys()])
counts_15 = [data["hourly_distribution_2015"][str(h)] for h in hours_15]

hours_24 = sorted([int(k) for k in data["hourly_distribution_2024"].keys()])
counts_24 = [data["hourly_distribution_2024"][str(h)] for h in hours_24]

plt.figure(figsize=(10, 5))
plt.plot(hours_15, counts_15, label="2015 (Yellow Taxi)", marker='o', color='orange', linewidth=2)
plt.plot(hours_24, counts_24, label="2024 (Yellow Taxi)", marker='s', color='navy', linewidth=2)
plt.title("Hourly Taxi Demand Distribution (2015 vs 2024)", fontsize=12, fontweight='bold')
plt.xlabel("Hour of Day (0-23)", fontsize=10)
plt.ylabel("Number of Trips", fontsize=10)
plt.xticks(range(0, 24))
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend()
plt.tight_layout()
plt.savefig("hourly_distribution.png", dpi=300)
plt.close()

# ==========================================
# ΔΙΑΓΡΑΜΜΑ 2: Ημερήσια Κατανομή (2024)
# ==========================================
days_24 = sorted([int(k) for k in data["daily_distribution_2024"].keys()])
day_counts_24 = [data["daily_distribution_2024"][str(d)] for d in days_24]

plt.figure(figsize=(11, 4))
plt.bar(days_24, day_counts_24, color='skyblue', edgecolor='black', alpha=0.8)
plt.title("Daily Taxi Demand Distribution for 2024", fontsize=12, fontweight='bold')
plt.xlabel("Day of Month (1-31)", fontsize=10)
plt.ylabel("Number of Trips", fontsize=10)
plt.xticks(range(1, 32))
plt.grid(axis='y', linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("daily_distribution_2024.png", dpi=300)
plt.close()

# ==========================================
# ΔΙΑΓΡΑΜΜΑ 3: Ιστόγραμμα Trip Distance (Λογαριθμική Κλίμακα)
# ==========================================
dist_buckets = sorted([int(k) for k in data["trip_distance_buckets_2024"].keys()])
dist_counts = [data["trip_distance_buckets_2024"][str(b)] for b in dist_buckets]

plt.figure(figsize=(10, 4))
plt.bar(dist_buckets, dist_counts, color='salmon', edgecolor='black', width=0.9)
plt.yscale('log') # Υποχρεωτική λογαριθμική κλίμακα βάσει εκφώνησης
plt.title("Trip Distance Distribution for 2024 (Log Scale)", fontsize=12, fontweight='bold')
plt.xlabel("Distance Bucket (Floored to Nearest Mile)", fontsize=10)
plt.ylabel("Frequency (Log Count)", fontsize=10)
plt.grid(True, which="both", linestyle="--", alpha=0.3)
plt.tight_layout()
plt.savefig("trip_distance_histogram.png", dpi=300)
plt.close()

# ==========================================
# ΔΙΑΓΡΑΜΜΑ 4: Ιστόγραμμα Total Amount (Λογαριθμική Κλίμακα)
# ==========================================
amt_buckets = sorted([int(k) for k in data["total_amount_buckets_2024"].keys()])
amt_counts = [data["total_amount_buckets_2024"][str(b)] for b in amt_buckets]

plt.figure(figsize=(10, 4))
plt.bar(amt_buckets, amt_counts, color='emerald' if 'emerald' in plt.colormaps else 'green', edgecolor='black', width=8)
plt.yscale('log') # Υποχρεωτική λογαριθμική κλίμακα βάσει εκφώνησης
plt.title("Total Amount Distribution for 2024 (Log Scale)", fontsize=12, fontweight='bold')
plt.xlabel("Total Amount Bucket (Steps of $10)", fontsize=10)
plt.ylabel("Frequency (Log Count)", fontsize=10)
plt.grid(True, which="both", linestyle="--", alpha=0.3)

# Προσθήκη κειμένου (annotation) για τις ανωμαλίες / αρνητικές τιμές
neg_count = data.get("negative_total_amount_count_2024", 0)
if neg_count > 0:
    plt.annotate(f"Anomalies: {neg_count} negative values detected", 
                 xy=(0.05, 0.9), xycoords='axes fraction',
                 color='red', weight='bold', bbox=dict(boxstyle="round,pad=0.3", fc="yellow", alpha=0.5))

plt.tight_layout()
plt.savefig("total_amount_histogram.png", dpi=300)
plt.close()

print("Επιτυχία! Δημιουργήθηκαν 4 αρχεία PNG έτοιμα για την αναφορά:")
print("- hourly_distribution.png\n- daily_distribution_2024.png\n- trip_distance_histogram.png\n- total_amount_histogram.png")
