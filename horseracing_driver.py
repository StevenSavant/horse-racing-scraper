import os
from numpy import empty
import pandas as pd
from dbhandler import *
from datetime import date
from scrape_transform import *
from horseracing_scrape import horse_racing_scrape
from utils import log_blue, log_info, log_warn, set_log_level, log_debug, log_error, log_success

DB_USERNAME = os.getenv("DB_USERNAME", None)
DB_PASSWORD = os.getenv("DB_PASSWORD", None)
DB_ADDRESS = os.getenv("DB_ADDRESS", None)
DB_PORT = os.getenv("DB_PORT", None)
DB_NAME = os.getenv("DB_NAME", None)
DB_TYPE = os.getenv("DB_TYPE", None)
DB_INSERTS = os.getenv("DB_INSERTS", "No")

db_engine = get_engine(DB_NAME, DB_TYPE, DB_ADDRESS, DB_USERNAME, DB_PASSWORD, DB_PORT)
today = date.today()
today_label = today.strftime("%Y-%m-%d")
set_log_level("INFO")

# ha stands for Horse API - The Other data source
if not db_engine:
    log_error('Failed to start database engine, please check environment variable DB_ parameters')
    exit()


def append_track_ids(row, db_records):
    track_name = row['track_name']
    try:
        row['id'] = db_records.loc[track_name]['id']
    except:
        log_error(f'failed to match track {track_name} with a database id')


def append_race_ids(row, db_records):
    x, y, z = row['race_date'], row['fk_track_id'], row['race_num']
    try:
        result = db_records.query(f"fk_track_id == {y} & race_num == '{z}'", engine='python')
        if not result.empty:
            row['id'] = result.iloc[0]['id']
    except:
        log_error(f'failed to match race for date: {x}, on track: {y}, race number: {z}')


def append_bet_type_ids(row, db_records):
    y, z = row['fk_race_id'], row['bet_type']
    try:
        result =db_records.query(f"fk_race_id == {y} & bet_type == '{z}'", engine='python')
        if not result.empty:
            row['id'] = result.iloc[0]['id']
    except Exception as e:
        log_error(f'failed to match bet type for pair: {y}, {z} : {e}')


def append_horse_ids(row, db_records):
    xrow = row.copy()
    x = xrow['horse_name']
    x = x.lower()
    try:
        result = db_records.query(f'name == "{x}"', engine='python')
        if not result.empty:
            xrow['horse_id'] = result.loc[result.index[0], 'id']
            return xrow
        else:
            log_blue(f'could not find match for horse name: {x}')
    except:
        log_error(f'failed to match horse name: {x} to id')


def append_race_res_ids(row, db_records):
    u_row = row.copy()
    y, z = u_row['race_id'], u_row['horse_id']
    try:
        result =db_records.query(f"race_id == {y} & horse_id == {z}", engine='python')
        if not result.empty:
            u_row['id'] = result.iloc[0]['id']
            return u_row
    except Exception as e:
        log_error(f'failed to append race result ids for race: {y}, horse {z}, horse name: {row["horse_name"]} with error: {e}')


def get_update_confirmation(table, action, report_df):
            print(f'\n{action.upper()} : {table}\n{report_df}\n')
            answ = input(f'These reocrds will be {action}ed in the {table} table. Proceed? (type "Yes" to confirm):  ')
            return bool(answ == 'Yes')


def main(update=False, inserts=False, local_run=False):
    global db_engine
    scrape_data = None

    # Override date for testing
    # today_label = '2022-07-08'

    # <----------- Scraper ----------- >

    scrape_data = horse_racing_scrape([today_label])
    log_info(f'Scrapping complete: {list(scrape_data.keys())}')

    # <----------- Tracks ----------- >

    track_scrape_records = build_scraped_tracks(scrape_data)
    query, params = build_tracks_query(track_scrape_records)
    
    log_debug(query)
    log_debug(params)

    try:
        track_db_records = pd.read_sql(sql=query, con=db_engine, params=params)
    except Exception as e:
        log_error(f'Failed to read database tracks: {e}')
        exit('Aborting Sync')

    track_db_records.set_index('track_name', inplace=True)
    track_scrape_records.apply(append_track_ids, axis=1, db_records=track_db_records)  ## There's a better way to do this

    log_blue(f'\n{track_db_records}\n')
    log_success(f'\n{track_scrape_records[["id"]]}\n')
    existing_tracks = track_scrape_records.query('id != ""')


    # <----------- Races ----------- >

    race_scrape_records = build_scaped_races(scrape_data=scrape_data, tracks_df=track_scrape_records)
    query, params = build_races_day_query(race_scrape_records, existing_tracks, today_label)
    
    log_debug(query)
    log_debug(params)

    try:
        race_db_records = pd.read_sql(sql=query, con=db_engine, params=params, coerce_float=False)
        race_scrape_records.apply(append_race_ids, axis=1, db_records=race_db_records)  ## There's a better way to do this
    except Exception as e:
        log_error(f'Failed to read database races: {e}')
        exit('Aborting Sync')

    log_blue(f"\n{race_db_records[['race_date', 'fk_track_id', 'race_num', 'off_at_time', 'race_class', 'race_sex', 'purse_usd_size', 'id']]}\n")
    log_success(f"\n{race_scrape_records[['race_date', 'fk_track_id', 'race_num', 'off_at_time', 'race_class', 'race_sex', 'purse_usd_size', 'id']]}\n")


    # <----------- Bet Type ----------- >

    bet_types_scrape_records = build_scraped_bet_types(scrape_data, race_scrape_records, track_scrape_records)
    bet_types_db_records = pd.DataFrame(columns=['fk_race_id', 'bet_type', 'id'])

    if not bet_types_scrape_records.empty:
        query, params = build_bet_type_query(bet_types_scrape_records)

        log_debug(query)
        log_debug(params)

        try:
            bet_types_db_records = pd.read_sql(sql=query, con=db_engine, params=params, coerce_float=False)
        except Exception as e:
            log_error(f'Failed to read database bet types: {e}')
            exit('Aborting Sync')

        bet_types_db_records.reset_index(inplace=True)
        bet_types_scrape_records.apply(append_bet_type_ids, axis=1, db_records=bet_types_db_records)

        log_blue(f"\n{bet_types_db_records}\n")
        log_success(f"\n{bet_types_scrape_records}\n")

    
    # <--------- Race Results ----------- >

    race_results_scrape_records = build_race_results(scrape_data, race_scrape_records, track_scrape_records)
    race_res_db_records = pd.DataFrame(columns=['race_id', 'horse_id', 'pgm', 'fin_place', 'id'])

    if not race_results_scrape_records.empty:
        query, params = build_horse_query(race_results_scrape_records)

        log_debug(f'\n{query}\n')
        log_debug(f'\n{params}\n')

        try:
            horses_db_records = pd.read_sql(sql=query, con=db_engine, params=params, coerce_float=False)
            horses_db_records.dropna(inplace=True)
        except Exception as e:
            log_error(f'Failed to read database horses: {e}')
            exit('Aborting Sync')
        
        horses_db_records['name'] = horses_db_records['name'].str.lower()
        race_results_scrape_records = race_results_scrape_records.apply(append_horse_ids, axis=1, db_records=horses_db_records)
        race_results_scrape_records['race_id'] = race_results_scrape_records['race_id'].fillna(0)
        race_results_scrape_records['race_id'] = race_results_scrape_records['race_id'].astype(int)
        race_results_scrape_records['horse_id'] = race_results_scrape_records['horse_id'].fillna(0)
        race_results_scrape_records['horse_id'] = race_results_scrape_records['horse_id'].astype(int)
        race_results_scrape_records['pgm'] = race_results_scrape_records['pgm'].fillna(0)
        race_results_scrape_records['pgm'] = race_results_scrape_records['pgm'].astype(int)
        race_results_scrape_records['fin_place'] = race_results_scrape_records['fin_place'].fillna(0)
        race_results_scrape_records['fin_place'] = race_results_scrape_records['fin_place'].astype(int)

        log_blue(f"\n{horses_db_records.sort_values(by='name')}")
        log_success(race_results_scrape_records[['horse_name', 'horse_id']].sort_values(by='horse_name'))
        race_results_scrape_records.dropna(inplace=True)

        query, params = build_race_results_query(race_results_scrape_records)

        log_debug(f'\n\n{query}\n\n')
        log_debug(f'\n\n{params}\n\n')

        try:
            race_res_db_records = pd.read_sql(sql=query, con=db_engine, params=params, coerce_float=False)
        except Exception as e:
            log_error(f'Failed to read database race results: {e}')
            exit('Aborting Sync')
        race_results_scrape_records = race_results_scrape_records.apply(append_race_res_ids, axis=1, db_records=race_res_db_records)

        log_blue(f'\n{race_res_db_records}')
        log_success(f'\n{race_results_scrape_records}')


    # <--------- Export Report ----------- >

    missing_tracks = track_scrape_records.query("id == ''")
    missing_races = race_scrape_records.query("id == ''")
    missing_race_results = race_results_scrape_records.query("id == ''")
    missing_bet_types = bet_types_scrape_records.query("id == ''")

    if not missing_tracks.empty:
        log_error(f'\nMISSING TRACKS:\n{missing_tracks}')
    if not missing_races.empty:
        log_error(f'\nMISSING RACES:\n{missing_races}')
    if not missing_race_results.empty:
        log_error(f'\nMISSING RACE RESULTS:\n{missing_race_results}')

    export_missing_data = {
        "Missing Races" :  {'table' : 'races', 'records' : missing_races},
        "Missing Results" : {'table' : 'race_results', 'records' : missing_race_results},
        "Missing Tracks" : {'table' : 'race_tracks', 'records' : missing_tracks},
        "Missing Bet Types" : {'table' : 'mapped_race_bet_types', 'records' : missing_bet_types}
    }

    race_scrape_records['source'] = race_results_scrape_records['source'] = bet_types_scrape_records['source'] = 'HRNation'
    race_db_records['source'] = race_res_db_records['source'] = bet_types_db_records['source'] = 'Database'

    # Merge Race
    race_sync_columns = ['source', 'id', 'fk_track_id', 'race_num', 'race_class', 'race_sex', 'off_at_time', 'race_track_surf', 'purse_usd_size']
    merged_race_data = pd.concat([race_scrape_records[race_sync_columns], race_db_records[race_sync_columns]], ignore_index=True)
    merged_race_data = merged_race_data.query('id != ""')
    if not merged_race_data.empty:
        merged_race_data = merged_race_data[merged_race_data.groupby('id').id.transform('count') > 1]

        # At random times... the Race Number may not be a race number
        merged_race_data = merged_race_data[merged_race_data.id.apply(lambda x: str(x).isnumeric())]
        merged_race_data = merged_race_data[merged_race_data.race_num.apply(lambda x: str(x).isnumeric())]
        merged_race_data['id'], merged_race_data['race_num'] = merged_race_data['id'].astype(int), merged_race_data['race_num'].astype(int)
        merged_race_data.drop_duplicates(subset=race_sync_columns[1:], keep=False, inplace=True)

    # Merge Bet Types
    btypes_sync_columns = ['source', 'id', 'fk_race_id', 'bet_type']
    merged_btypes_data = pd.concat([bet_types_scrape_records[btypes_sync_columns], bet_types_db_records[btypes_sync_columns]], ignore_index=True)
    merged_btypes_data = merged_btypes_data.query('id != ""')
    if not merged_btypes_data.empty:
        merged_btypes_data = merged_btypes_data[merged_btypes_data.groupby('id').id.transform('count') > 1]
        merged_btypes_data['id'], merged_btypes_data['fk_race_id'] = merged_btypes_data['id'].astype(int), merged_btypes_data['fk_race_id'].astype(int)
        merged_btypes_data.drop_duplicates(subset=btypes_sync_columns[1:], keep=False, inplace=True)

    # Merge Race Results
    res_sync_columns = ['source', 'id', 'race_id', 'horse_id', 'pgm', 'fin_place']
    merged_res_data = pd.concat([race_results_scrape_records[res_sync_columns], race_res_db_records[res_sync_columns]], ignore_index=True)
    merged_res_data = merged_res_data.query('id != ""')
    if not merged_res_data.empty:
        merged_res_data = merged_res_data[merged_res_data.groupby('id').id.transform('count') > 1]
        merged_res_data['id'], merged_res_data['horse_id'] = merged_res_data['id'].astype(int), merged_res_data['horse_id'].astype(int)
        merged_res_data.drop_duplicates(subset=res_sync_columns[1:], keep=False, inplace=True)

    empties = [merged_race_data.empty, merged_res_data.empty, merged_btypes_data.empty]
    empties += [x['records'].empty for x in export_missing_data.values() if 'records' in x]
    if not all(empties):
        with pd.ExcelWriter(f'{today_label}-HRNation-2.xlsx') as writer:
            
            if not merged_race_data.empty:
                sorted_race_data = merged_race_data.sort_values(by=['id', 'source'])
                sorted_race_data.to_excel(writer, sheet_name='Race Differences')

            if not merged_res_data.empty:
                sorted_res_data = merged_res_data.sort_values(by=['id', 'source'])
                sorted_res_data.to_excel(writer, sheet_name='Results Differences')

            if not merged_btypes_data.empty:
                sorted_btypes_data = merged_btypes_data.sort_values(by=['id', 'source'])
                sorted_btypes_data.to_excel(writer, sheet_name='Bet Type Differences')

            [export_missing_data[k]['records'].to_excel(writer, sheet_name=k) for k, v in export_missing_data.items() if not export_missing_data[k]['records'].empty]
    else:
        log_success('Nothing to update! Database is Synced!')



    if update:
        log_info('Attempting Database Update in place')
        log_warn('Feature not Ready')

    log_info('Inserts:' + inserts)
    if inserts == 'Yes':
        log_blue('Inserting missing records')
        export_missing_data['Missing Results']['records'].drop(columns='horse_name', inplace=True)


        for k, v in export_missing_data.items():

            if v['records'].empty:
                log_success(f'No Missing data in table {v["table"]}!')
                continue

            if local_run:
                resp = get_update_confirmation(v['table'], 'Insert', v['records'])
                if resp == False:
                    log_info(f"Skipping table insert: {v['table']}")
                    break
            log_blue('Proceeding with insert')

            try:
                v['records'].drop(columns='id', inplace=True)
                changes = v['records'].to_sql(v['table'], con=db_engine, if_exists='append', index=False)
                log_success(f'{changes} inserted to {v["table"]} table')
            except Exception as e:
                log_error(f'Failed to insert missing records: {v["table"]} with error: {e}')

    log_success('Sync Run Complete!')


if __name__ == "__main__":
    log_debug('Starting Horse-Racing-Nation Scraper')
    # resp = get_update_confirmation('tablename', 'Insert', pd.DataFrame(columns=['test1', 'test2', 'test3']))
    # print(resp)
    main(inserts=DB_INSERTS, local_run=True)