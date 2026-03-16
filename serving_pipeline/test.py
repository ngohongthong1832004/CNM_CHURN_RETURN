import pandas as pd
import numpy as np

# Load your data
reference_df = pd.read_csv('data_model/reference/reference_data.csv')
current_df = pd.read_csv('data_model/production/current_data.csv')

print("=== AGE STATISTICS ===")
print(f"\nReference Age:")
print(f"  Count: {len(reference_df)}")
print(f"  Min: {reference_df['Age'].min()}")
print(f"  Max: {reference_df['Age'].max()}")
print(f"  Mean: {reference_df['Age'].mean():.2f}")
print(f"  Std: {reference_df['Age'].std():.2f}")

print(f"\nCurrent Age:")
print(f"  Count: {len(current_df)}")
print(f"  Min: {current_df['Age'].min()}")
print(f"  Max: {current_df['Age'].max()}")
print(f"  Mean: {current_df['Age'].mean():.2f}")
print(f"  Std: {current_df['Age'].std():.2f}")

# Simulate Evidently's binning
n_bins = 150

# Reference
ref_sorted = reference_df['Age'].sort_values().reset_index(drop=True)
ref_bins = pd.qcut(range(len(ref_sorted)), n_bins, labels=False, duplicates='drop')
ref_df_binned = pd.DataFrame({'age': ref_sorted, 'bin': ref_bins})
ref_means = ref_df_binned.groupby('bin')['age'].mean()

# Current
curr_sorted = current_df['Age'].sort_values().reset_index(drop=True)
curr_bins = pd.qcut(range(len(curr_sorted)), n_bins, labels=False, duplicates='drop')
curr_df_binned = pd.DataFrame({'age': curr_sorted, 'bin': curr_bins})
curr_means = curr_df_binned.groupby('bin')['age'].mean()

print("\n=== BINNING RESULTS ===")
print(f"Reference bins: {len(ref_means)}")
print(f"Current bins: {len(curr_means)}")

print(f"\nReference bin means (first 10):")
print(ref_means.head(10))

print(f"\nCurrent bin means (first 10):")
print(curr_means.head(10))

# Calculate variance
print(f"\nVariance in bin means:")
print(f"Reference: {ref_means.var():.4f}")  # Should be VERY LOW
print(f"Current: {curr_means.var():.4f}")    # Should be HIGHER

# Plot
import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Plot 1: Binned means (giống Evidently)
axes[0].plot(ref_means.index, ref_means.values, 
             color='green', linewidth=2, label='Reference', alpha=0.7)
axes[0].fill_between(ref_means.index, 
                     reference_df['Age'].mean() - reference_df['Age'].std(),
                     reference_df['Age'].mean() + reference_df['Age'].std(),
                     color='green', alpha=0.2)
axes[0].plot(curr_means.index, curr_means.values, 
             color='red', linewidth=2, label='Current', alpha=0.7)
axes[0].set_xlabel('Index binned')
axes[0].set_ylabel('Age (mean)')
axes[0].set_title('Drift Visualization (Evidently style)')
axes[0].legend()
axes[0].grid(alpha=0.3)

# Plot 2: Histograms
axes[1].hist(reference_df['Age'], bins=50, alpha=0.5, 
             color='green', edgecolor='black', label='Reference')
axes[1].hist(current_df['Age'], bins=50, alpha=0.5, 
             color='red', edgecolor='black', label='Current')
axes[1].axvline(reference_df['Age'].mean(), color='darkgreen', 
                linestyle='--', linewidth=2, label=f"Ref Mean: {reference_df['Age'].mean():.1f}")
axes[1].axvline(current_df['Age'].mean(), color='darkred', 
                linestyle='--', linewidth=2, label=f"Curr Mean: {current_df['Age'].mean():.1f}")
axes[1].set_xlabel('Age')
axes[1].set_ylabel('Frequency')
axes[1].set_title('Age Distribution Comparison')
axes[1].legend()
axes[1].grid(alpha=0.3)

plt.tight_layout()
plt.savefig('age_drift_analysis.png', dpi=150)
print("\n✓ Saved: age_drift_analysis.png")