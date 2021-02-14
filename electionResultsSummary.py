import pandas as pd

results = pd.read_csv('election_2021_01_05_runoff_precinct.csv')
results.groupby(['race','candidate']).sum().to_csv('election_2021_01_05_runoff_summary.csv')
results[results['race']=='US Senate (Perdue)'].groupby(['county']).sum().to_csv('election_2021_01_05_runoff_county.csv')