import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# Combine Spark output CSV fragments into a single local dataframe
csv_files = glob.glob("reports/analytics_csv/*.csv")
if not csv_files:
    print("[ERROR] Run the streaming pipeline first to gather log data!")
    exit()

df_list = [pd.read_csv(f, names=["window_start", "window_end", "sensor_id", "avg_temp", "max_temp", "records"]) for f in csv_files]
df = pd.concat(df_list).dropna().sort_values("window_start")

# Plot 1: Average Temperature Trend Over Time per Sensor
plt.figure(figsize=(10, 5))
for sensor in df["sensor_id"].unique()[:3]: # Track a few sensors for clean visuals
    sub = df[df["sensor_id"] == sensor]
    plt.plot(sub["window_start"], sub["avg_temp"], marker='o', label=sensor)

plt.title("Event-Time Processing: Average Temperature per Window Window")
plt.xlabel("Window Start Time")
plt.ylabel("Temperature (°C)")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("reports/event_time_analytics_plot.png")
print("[SUCCESS] Report plot generated safely inside reports/event_time_analytics_plot.png")
