
import pandas as pd

from utils import log_error
track_columns = ['track_name', 'id']
horses_columns = ['name', 'id']
race_results_columns = ['race_id', 'horse_name', 'horse_id', 'pgm', 'fin_place', 'id']
race_columns = ['race_date', 'fk_track_id', 'race_num', 'off_at_time', 'race_track_dist', 'race_track_surf', 'race_class', 'race_sex', 'purse_usd_size', 'id']
bet_type_columns = ['fk_race_id', 'bet_type', 'id']


def build_scraped_tracks(scrape_data):
    df = pd.DataFrame( columns=track_columns)
    for day in scrape_data:
        for track_name in scrape_data[day]:
            record = {x : track_name if x == track_columns[0] else '' for x in track_columns}
            df.loc[track_name] = record
    
    return df


def build_scaped_races(scrape_data, tracks_df):
    df = pd.DataFrame( columns=race_columns)
    for day in scrape_data:
        for track_name in scrape_data[day]:
            for race_num in scrape_data[day][track_name]:
                
                if race_num == 'id':
                    continue

                if not tracks_df.loc[track_name]['id']:
                    break

                record = {x : '' for x in race_columns}
                record['race_date'] = day
                record['fk_track_id'] = tracks_df.loc[track_name]['id']
                record['race_num'] = race_num
                record['off_at_time'] = scrape_data[day][track_name][race_num]['ap']['Race Time']
                record['race_track_dist'] = scrape_data[day][track_name][race_num]['ap']['Length']
                record['race_track_surf'] = scrape_data[day][track_name][race_num]['ap']['Surface']
                record['race_class'] = scrape_data[day][track_name][race_num]['ap']['Race Class']
                record['race_sex'] = scrape_data[day][track_name][race_num]['ap']['Sex']
                record['purse_usd_size'] = scrape_data[day][track_name][race_num]['ap']['Purse']
                df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
    
    return df


def build_scraped_bet_types(scrape_data, race_df, track_df):
    df = pd.DataFrame( columns=bet_type_columns)
    for day in scrape_data:
        for track_name in scrape_data[day]:
            for race_num in scrape_data[day][track_name]:
                if race_num == 'id':
                    continue

                if not track_df.loc[track_name]['id']:
                    break

                race_row = race_df.query(f"fk_track_id == {track_df.loc[track_name]['id']} & race_num == '{race_num}'", engine='python')
                if race_row.empty:
                    continue
                
                try:
                    bet_types = scrape_data[day][track_name][race_num]['pool'].to_dict()
                except Exception as e:
                    # Not all races have pool listings, and the data type is inconsistent
                    # TODO: fix this in the scrape models
                    continue

                if race_row.empty or not (race_row.iloc[0]['id']):
                    # not enough data to decide if it's missing
                    continue

                for v in bet_types['Pool'].values():
                    record = {x : '' for x in bet_type_columns}
                    record['fk_race_id'] = race_row.iloc[0]['id']
                    record['bet_type'] = v.upper()
                    df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
    
    return df


def build_race_results(scrape_data, race_df, track_df):
    df = pd.DataFrame( columns=race_results_columns)
    for day in scrape_data:
        for track_name in scrape_data[day]:
            for race_num in scrape_data[day][track_name]:

                if race_num == 'id':
                    continue
                
                if not track_df.loc[track_name]['id']:
                    break

                race_row = race_df.query(f"fk_track_id == {track_df.loc[track_name]['id']} & race_num == '{race_num}'", engine='python')

                if race_row.empty or not (race_row.iloc[0]['id']):
                    # This race is not in the database at all
                    continue

                # The 'runners' table (if it exists) holds the actual race results
                try:
                    runners = scrape_data[day][track_name][race_num]['runners']
                    runners = runners.rename({'Runner':'horse_name', 'Horse Number' : 'pgm'}, axis='columns')
                    runners.index += 1
                    runners.index.name = 'fin_place'
                    runners.reset_index(inplace=True)
                    runners['id'] = ''
                    runners['race_id'] = race_row.iloc[0]['id']
                    runners['horse_id'] = ''
                    df = pd.concat([df, runners[['race_id', 'horse_name', 'horse_id', 'fin_place', 'pgm', 'id']]], ignore_index=True)
                except Exception as e:
                    continue

    return df 