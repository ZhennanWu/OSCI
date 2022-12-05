import calendar
import datetime
import pandas as pd
from pandas import DataFrame

commits_dfs = []
repository_dfs = []
company_contributed_repository_counts = []
community_dfs = []
for year in [2019]:
    for month in range(1, 13):
        last_day_in_month = datetime.datetime(
            year, month, calendar.monthrange(year, month)[1])
        df = pd.read_csv(f'.data/public/report/OSCI_commits_ranking_MTD/OSCI_commits_ranking_MTD_{last_day_in_month.strftime("%Y-%m-%d")}.csv')
        df['month'] = f'{year}-{month:02d}'
        commits_dfs.append(df)
        df = pd.read_csv(f'.data/public/report/Repository_Composition_Ranking_MTD/Repository_Composition_Ranking_MTD_{last_day_in_month.strftime("%Y-%m-%d")}.csv')
        df['month'] = f'{year}-{month:02d}'
        repository_dfs.append(df)
        repos = set()
        for i, row in df.iterrows():
            if row['company'] != 'Unknown':
                repos.add(row['repo_name'])
        company_contributed_repository_counts.append([len(repos), f'{year}-{month:02d}'])

        df = pd.read_csv(f'.data/public/report/OSCI_ranking_MTD/OSCI_ranking_MTD_{last_day_in_month.strftime("%Y-%m-%d")}.csv')
        df['month'] = f'{year}-{month:02d}'
        community_dfs.append(df)

commits_df = pd.concat(commits_dfs)
commits_df.to_json(".data/public/report/OSCI_commits_ranking_MTD/OSCI_commits_ranking_MTD.json", orient='records')

repository_df = pd.concat(repository_dfs)
repository_df.to_json(".data/public/report/Repository_Composition_Ranking_MTD/Repository_Composition_Ranking_MTD.json", orient='records')

pd.DataFrame(company_contributed_repository_counts, columns=["x", "y"]).to_json(".data/public/report/Repository_Composition_Ranking_MTD/Company_Contributed_Repo_Count_MTD.json", orient='records')


community_df = pd.concat(community_dfs)
community_df.to_json(".data/public/report/OSCI_ranking_MTD/OSCI_ranking_MTD.json", orient='records')