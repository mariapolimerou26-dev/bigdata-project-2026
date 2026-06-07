import numpy as np
import matplotlib.pyplot as plt

# =====================================================================
# ΓΡΑΦΗΜΑ 1: Demand Profile (Q1)
# =====================================================================
hours = [6, 7, 8, 9]
day1_trips = [6637, 6011, 5586, 6977]
day2_trips = [5811, 8256, 10946, 13373]
day3_trips = [4120, 5216, 7908, 11869]

plt.figure(figsize=(10, 6))
plt.plot(hours, day1_trips, marker='o', linewidth=2.5, label='Ημέρα 1', color='#1f77b4')
plt.plot(hours, day2_trips, marker='s', linewidth=2.5, label='Ημέρα 2', color='#ff7f0e')
plt.plot(hours, day3_trips, marker='^', linewidth=2.5, label='Ημέρα 3', color='#2ca02c')

plt.title('Κατανόηση Ζήτησης (Demand Profile) ανά Ώρα και Ημέρα - Q1', fontsize=14, fontweight='bold', pad=15)
plt.xlabel('Ώρα της Ημέρας (pickup_hour)', fontsize=12)
plt.ylabel('Πλήθος Διαδρομών (trips)', fontsize=12)
plt.xticks(hours)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(fontsize=11)
plt.tight_layout()
plt.savefig('q1_chart.png', dpi=300)
plt.close()
print("-> Το Γράφημα 1 (q1_chart.png) δημιουργήθηκε επιτυχώς!")

# =====================================================================
# ΓΡΑΦΗΜΑ 2: Πείραμα Κλιμάκωσης (Q6)
# =====================================================================
configurations = ['Διάταξη Α\n(2 Executors)', 'Διάταξη Β\n(4 Executors)', 'Διάταξη C\n(8 Executors)']
cold_runs = [0, 353.99, 343.76]  
warm_runs = [343.74, 343.65, 353.88]

x = np.arange(len(configurations))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 6))
rects1 = ax.bar(x - width/2, cold_runs, width, label='Cold Run', color='#d62728', alpha=0.85)
rects2 = ax.bar(x + width/2, warm_runs, width, label='Warm Run', color='#1f77b4', alpha=0.85)

ax.set_title('Πείραμα Κλιμάκωσης: Χρόνοι Εκτέλεσης ανά Διάταξη - Q6', fontsize=14, fontweight='bold', pad=15)
ax.set_xlabel('Διατάξεις Cluster (Executors)', fontsize=12)
ax.set_ylabel('Χρόνος Εκτέλεσης (Δευτερόλεπτα)', fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels(configurations)
ax.legend(fontsize=11)
ax.grid(axis='y', linestyle='--', alpha=0.6)

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        if height > 0:
            ax.annotate(f'{height:.2f}s',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=10, fontweight='bold')

autolabel(rects1)
autolabel(rects2)

plt.tight_layout()
plt.savefig('q6_chart.png', dpi=300)
plt.close()
print("-> Το Γράφημα 2 (q6_chart.png) δημιουργήθηκε επιτυχώς!")
