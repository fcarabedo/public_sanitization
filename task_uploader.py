import pandas as pd
import os

file_path = './nitin/batch_77_data_samples_without_issue.csv'

df = pd.read_csv(file_path)

cols_to_drop = ['base_dockerfile',
       'base_image_name', 'run_script_content', 'selected_test_files_to_run',
       'parsing_script_content', 'build_script_content', 'before_repo_set_cmd',
       'after_repo_set_cmd', 'instance_dockerfile', 'before_test_log_cds',
       'after_test_log_cds', 'before_test_result_cds', 'after_test_result_cds',
       'fail_to_pass_select', 'pass_to_pass_select', 'fail_to_pass_full',
       'pass_to_pass_full', 'evaluation_time', 'tar_cds']


# raw_name : taxo_name

column_mapping = {
       
    "pr_url": "url_pr",
    "pr_message": "initial_problem_statement",
    "gold_patch_summary": "golden_patch",
    "issue_0": "synthetic_rewrite_problem_statement",
    "requirement_0": "synthetic_requirement",
    "hint_0": "synthetic_hint",
    "plan_0": "synthetic_plan",
    "issue_1": "synthetic_rewrite_problem_statement_1",
    "requirement_1": "synthetic_requirement_1",
    "hint_1": "synthetic_hint_1",
    "plan_1": "synthetic_plan_1",
    "issue_2": "synthetic_rewrite_problem_statement_2",
    "requirement_2": "synthetic_requirement_2",
    "hint_2": "synthetic_hint_2",
    "plan_2": "synthetic_plan_2",
    "gold_patch": "diff",
    "test_patch": "test_diff",
}

# Read the CSV file
df.drop(columns=cols_to_drop, inplace=True)

# Rename columns according to the mapping
df = df.rename(columns=column_mapping)

# Add languageCode column with value 'en_US'
df['languageCode'] = 'en_US'

# Build the dynamic output path
original_filename = os.path.basename(file_path)
output_dir = '/Users/fernando.carabedo/Desktop/sweap/evals/raw'
output_path = os.path.join(output_dir, original_filename)

# Save the modified dataframe to the new CSV
df.to_csv(output_path, index=False)


def sample(file):
    df = pd.read_csv(file)
    print("Original dataset info:")
    print(f"Total rows: {len(df)}")
    
    # Filter records with src_file_lines_added >= 20
    df_filtered = df[df['src_file_lines_added'] >= 20]
    print(f"After filtering (src_file_lines_added >= 20): {len(df_filtered)} rows")
    print(f"Filtered out: {len(df) - len(df_filtered)} rows")
    
    # Use filtered dataframe for the rest of the function
    df = df_filtered
    
    print("\nDistribution of repo_name:")
    repo_counts = df['repo_name'].value_counts()
    print(repo_counts)

    # Sample 120 rows from each repo_name (or all rows if less than 120)
    sampled_dfs = []

    print("\nSampling process:")
    for repo_name in df['repo_name'].unique():
        repo_df = df[df['repo_name'] == repo_name]
        
        if len(repo_df) <= 120:
            sampled_df = repo_df
            print(f"{repo_name}: Taking all {len(repo_df)} rows")
        else:
            sampled_df = repo_df.sample(n=120, random_state=42)
            print(f"{repo_name}: Sampled 120 out of {len(repo_df)} rows")
        
        sampled_dfs.append(sampled_df)

    df_sampled = pd.concat(sampled_dfs, ignore_index=True)

    print(f"\nResults:")
    print(f"Original dataset size: {len(df)}")
    print(f"Sampled dataset size: {len(df_sampled)}")

    print("\nDistribution after sampling:")
    sampled_repo_counts = df_sampled['repo_name'].value_counts()
    print(sampled_repo_counts)

    # Construct the output file path
    original_filename = os.path.basename(file)
    filename_no_ext = os.path.splitext(original_filename)[0]
    output_dir = '/Users/fernando.carabedo/Desktop/sweap/evals/sampled'
    sampled_file_path = os.path.join(output_dir, f"{filename_no_ext}_sampled.csv")

    # Save the sampled dataset
    df_sampled.to_csv(sampled_file_path, index=False)
    print(f"\nSampled dataset saved to: {sampled_file_path}")


sample('./raw/batch_77_data_samples_with_issue.csv')
