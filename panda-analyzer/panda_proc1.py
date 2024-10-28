import pandas as pd

# Load the data from the input CSV file
input_file = 'panda_input.csv'  # Replace with your input file path
output_file = 'panda_output.csv'  # Replace with your desired output file path

# Read the CSV into a DataFrame
df = pd.read_csv(input_file, delimiter=';')

# Step 1: Calculate the total frequency of impressions for each unique URL
url_frequencies = df.groupby('url')['frequency of impressions'].sum().reset_index()
url_frequencies.columns = ['url', 'total_frequency']

# Merge the total frequency back into the original dataframe
df = pd.merge(df, url_frequencies, on='url')

# Step 2: Calculate the proportion of each label for every unique URL
def calculate_proportions(sub_df):
    total_frequency = sub_df['total_frequency'].iloc[0]

    # Define the correct order for labels
    correct_order = ['top-3', 'top-10', 'top-30', 'top-100', 'top-1000']

    # Handle zero or missing total_frequency to avoid division errors
    if total_frequency == 0 or pd.isna(total_frequency):
        return "No impressions available"

    label_counts = sub_df.groupby('label')['frequency of impressions'].sum()
    proportions = (label_counts / total_frequency * 100).round(2)  # Keep decimal proportions

    # Convert proportions to integers safely
    try:
        proportions = proportions.astype(int)
    except pd.errors.IntCastingNaNError:
        proportions = proportions.fillna(0).astype(int)

    # Create a dictionary with all labels in the correct order, defaulting to None
    ordered_proportions = {label: None for label in correct_order}

    # Update the dictionary with actual values only for labels with non-zero frequency
    for label in label_counts.index:
        if label_counts[label] > 0:
            ordered_proportions[label] = f"{label}: {proportions[label]}% fi: {label_counts[label]}"

    # Filter out None values and join the remaining results into a single string
    result = ', '.join([ordered_proportions[label] for label in correct_order if ordered_proportions[label] is not None])
    return result

# Apply the top share calculation for each URL and get unique values
url_results = df.groupby('url', group_keys=False).apply(calculate_proportions).reset_index()
url_results.columns = ['url', 'result']

# Replace "No impressions available" with the default fi: 0 for top-1000 only if there are no impressions at all
url_results['result'] = url_results['result'].replace("No impressions available", "top-1000: 0% fi: 0")

# Step 3: Calculate visibility based on grouped weights
def calculate_visibility(sub_df):
    # Define the weight categories based on the position
    def calculate_weight_grouped(position):
        if 1 <= position <= 3:
            return 0.60
        elif 4 <= position <= 10:
            return 0.30
        elif 11 <= position <= 100:
            return 0.10
        else:
            return 0.0

    # Apply the weights to the dataframe
    sub_df['grouped_weight'] = sub_df['position'].apply(calculate_weight_grouped)
    sub_df['grouped_visibility'] = sub_df['frequency of impressions'] * sub_df['grouped_weight']

    # Calculate the weighted visibility normalized by total frequency
    sub_df['normalized_visibility'] = sub_df['grouped_visibility'] / sub_df['total_frequency']

    # Return the sum of the normalized visibility for the URL
    weighted_visibility = sub_df['normalized_visibility'].sum()
    # Format visibility with comma as decimal delimiter
    return f"{weighted_visibility:.4f}".replace('.', ',')

# Calculate the visibility for each URL and aggregate unique values
visibility_results = df.groupby('url', group_keys=False).apply(calculate_visibility).reset_index()
visibility_results.columns = ['url', 'visibility']

# Merge the top share results and visibility results into a final DataFrame
final_result = pd.merge(url_results, visibility_results, on='url')

# Save the final result to a new CSV file
final_result.to_csv(output_file, index=False, sep=';', decimal=',')

print(f"Processed data has been saved to {output_file}")
