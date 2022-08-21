import os
import json
import pandas as pd
from dbhandler import *
from datetime import date
from scrape_transform import *
from horseracing_scrape import horse_racing_scrape
from utils import log_blue, log_info, log_warn, set_log_level, log_debug, log_error, log_success
from scrape_models import *

DB_USERNAME = os.getenv("DB_USERNAME", None)
DB_PASSWORD = os.getenv("DB_PASSWORD", None)
DB_ADDRESS = os.getenv("DB_ADDRESS", None)
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME", None)
DB_TYPE = os.getenv("DB_TYPE", None)
DB_UPDATES = os.getenv("DB_UPDATES", True)
DB_INSERTS = os.getenv("DB_INSERTS", True)
LOCAL_RUN = os.getenv("LOCAL_RUN", False)

set_log_level("INFO")
db_engine = get_engine(DB_NAME, DB_TYPE, DB_ADDRESS, DB_USERNAME, DB_PASSWORD, DB_PORT)
today = date.today()
today_label = today.strftime("%Y-%m-%d")


CONFIG = {}

# ha stands for Horse API - The Other data source
if not db_engine:
    log_error('Failed to start database engine, please check environment variable DB_ parameters')
    exit()


def _load_database_config():
    global CONFIG
    with open('database_config.json', 'r') as f:
        CONFIG = json.load(f)
    

def get_update_confirmation(table, action, report_df):
            print(f'\n{action.upper()} : {table}\n{report_df}\n')
            answ = input(f'These reocrds will be {action}ed in the {table} table. Proceed? (type "Yes" to confirm):  ')
            return bool(answ == 'Yes')


def print_missing_records(missing):
    for x in missing.values():
        if x['records'].empty:
            continue
        log_warn(f'\nMISSING {x["table"].upper()}\n{x["records"]}')


def query_horse_db(horse_df, scraped_race_res, db_engine):
    query, params = build_horse_query(
        race_res_df=scraped_race_res.get_dataframe()
    )

    log_debug(f'\n\n{query}\n{params}\n')

    #Query to Get Horse IDs for results
    try:
        horse_df = pd.read_sql(sql=query, con=db_engine, params=params)
        horse_df.dropna(inplace=True)
        horse_df['name'] = horse_df['name'].str.lower()
    except Exception as e:
        log_error(f'Failed to read database horses: {e}')
        exit('Aborting Sync')

    return horse_df


def query_trainer_db(trainers_df, scraped_race_res, db_engine):
    query, params = build_trainer_query(
        race_res_df=scraped_race_res.get_dataframe()
    )

    log_debug(f'\n\n{query}\n{params}\n')

    #Query to Get Horse IDs for results
    try:
        trainers_df = pd.read_sql(sql=query, con=db_engine, params=params)
        trainers_df.dropna(inplace=True)
        trainers_df['name'] = trainers_df['name'].str.lower()
    except Exception as e:
        log_error(f'Failed to read database horses: {e}')
        exit('Aborting Sync')

    return trainers_df


def query_jockey_db(jockey_df, scraped_race_res, db_engine):
    query, params = build_jockey_query(
        race_res_df=scraped_race_res.get_dataframe()
    )

    log_debug(f'\n\n{query}\n{params}\n')

    #Query to Get Horse IDs for results
    try:
        jockey_df = pd.read_sql(sql=query, con=db_engine, params=params)
        jockey_df.dropna(inplace=True)
        jockey_df['name'] = jockey_df['name'].str.lower()
    except Exception as e:
        log_error(f'Failed to read database horses: {e}')
        exit('Aborting Sync')

    return jockey_df

def compare_to_db(row, db_records):
    urow = row.copy()
    res = db_records.query(f'id == {row["id"]}')
    if not res.empty:
        if (not res.iloc[0]['fractional_times']) or (res.iloc[0]['fractional_times'] == ''):
            urow['needs_update'] = True
    
    return urow



def get_fractional_time_updates(scrp_races, database):
    scrp_races['needs_update'] = False
    if (not database.query('fractional_times == ""').empty) or (not database.query('fractional_times != fractional_times').empty):
        result = scrp_races.apply(compare_to_db, axis=1, db_records=database)
        result = result.dropna()
        return result.query('needs_update == True')
    else:
        log_warn('All Database records have fractinal times')
    return pd.DataFrame()


def main(update=False, inserts=False, local_run=False):
    global db_engine
    scrape_data = None
    _load_database_config()

    # Override date for testing
    # today_label = '2022-07-30'

    # <----------- Run Scraper ----------- >

    scrape_data = horse_racing_scrape([today_label], debug=True)
    log_info(f'Scrapping complete: {list(scrape_data.keys())}')


    # <----------- Collect Tracks ----------- >
    scraped_tracks = ScrapeTracks(
        scrape_data = scrape_data, 
        id_field    = CONFIG['TRACK_TABLE']['FIELDS']['ID'],
        name_field  = CONFIG['TRACK_TABLE']['FIELDS']['NAME'],
    )

    # Build DB Query
    query, params = build_tracks_query(
        tracks_df   = scraped_tracks.get_dataframe(),
        db_name     = DB_NAME,
        table_name  = CONFIG['TRACK_TABLE']['NAME'],
        name_field  = CONFIG['TRACK_TABLE']['FIELDS']['NAME']
    )
    
    log_debug(f'\n\n{query}\n{params}\n')

    # Get DB Tracks
    try:
        track_db_records = pd.read_sql(sql=query, con=db_engine, params=params)
    except Exception as e:
        log_error(f'Failed to read database tracks: {e}')
        exit('Aborting Sync')

    # Associate Existing Tracks
    scraped_tracks.attach_ids(track_db_records)

    log_blue(f'\n{track_db_records}\n')
    log_success(f'\n{scraped_tracks.get_dataframe()[["id"]]}\n')


#    # <----------- Races ----------- >
    scraped_races = ScrapeRaces(
        scrape_data=scrape_data, 
        tracks=scraped_tracks,
        id_field= CONFIG['RACES_TABLE']['FIELDS']['ID'],
        number_field=CONFIG['RACES_TABLE']['FIELDS']['NUMBER'],
        date_filed=CONFIG['RACES_TABLE']['FIELDS']['DATE'],
        sts_field=CONFIG['RACES_TABLE']['FIELDS']['STATUS'],
        dist_field=CONFIG['RACES_TABLE']['FIELDS']['DISTANCE'],
    )
    
    # Build Races Query
    query, params = build_races_query(
        races_df=scraped_races.get_dataframe(),
        tracks_df=scraped_tracks.get_dataframe(),
        table_name= CONFIG['RACES_TABLE']['NAME'],
        db_name= DB_NAME,
        race_day = today_label,
        race_date_field= CONFIG['RACES_TABLE']['FIELDS']['DATE'],
        fk_track_id_field = CONFIG['RACES_TABLE']['FIELDS']['TRACK_ID'],
        track_id = CONFIG['TRACK_TABLE']['FIELDS']['ID']
    )

    log_debug(f'\n\n{query}\n{params}\n')
    
    # Get DB Races
    try:
        races_db_records = pd.read_sql(sql=query, con=db_engine, params=params)
    except Exception as e:
        log_error(f'Failed to races database tracks: {e}')
        exit('Aborting Sync')


    # Associate Existing Tracks
    scraped_races.attach_ids(races_db_records)

    log_blue(f'\n{races_db_records}\n')
    log_success(f'\n{scraped_races.get_dataframe()[["race_date", "id", "race_sex", "purse_usd_size"]]}\n')

#    <----------- Wager Types ----------- >

    scraped_wager_types = ScrapeExoticBets(
        race_df=scraped_races.get_dataframe(),
        track_df=scraped_tracks.get_dataframe(),
        scrape_data = scrape_data,
        race_id='fk_race_id',
        type_field='bet_type',
        id_field='id'
    )
    bet_wager_db_records = pd.DataFrame(columns=['fk_race_id', 'bet_type', 'id'])

    if not scraped_wager_types.get_dataframe().empty:
        # Build Bet Types Query
        query, params = build_wager_query(
            wager_df=scraped_wager_types.get_dataframe()
        )

        log_debug(f'\n\n{query}\n{params}\n')

        try:
            bet_wager_db_records = pd.read_sql(sql=query, con=db_engine, params=params)
        except Exception as e:
            log_error(f'Failed to read database bet types: {e}')
            exit('Aborting Sync')

        bet_wager_db_records.reset_index(inplace=True)
        if not bet_wager_db_records.empty:
            scraped_wager_types.attach_ids(bet_wager_db_records)

        log_blue(f"\n{bet_wager_db_records}\n")
        log_success(f"\n{scraped_wager_types.get_dataframe()}\n")

#    <----------- Bet Type ----------- >


    scraped_bet_types = ScrapeBetTypes(
        scrape_data = scrape_data, 
        fk_race_id='fk_race_id',
        bet_type_field='bet_type',
        race_df=scraped_races.get_dataframe(),
        track_df=scraped_tracks.get_dataframe()
    )
    bet_types_db_records = pd.DataFrame(columns=['fk_race_id', 'bet_type', 'id'])

    if not scraped_bet_types.get_dataframe().empty:
        # Build Bet Types Query
        query, params = build_bet_type_query(
            bet_types_df=scraped_bet_types.get_dataframe()[['fk_race_id', 'bet_type', 'id']]
        )

        log_debug(f'\n\n{query}\n{params}\n')

        try:
            bet_types_db_records = pd.read_sql(sql=query, con=db_engine, params=params)
        except Exception as e:
            log_error(f'Failed to read database bet types: {e}')
            exit('Aborting Sync')

        bet_types_db_records.reset_index(inplace=True)
        if not bet_types_db_records.empty:
            scraped_bet_types.attach_ids(bet_types_db_records)

        log_blue(f"\n{bet_types_db_records}\n")
        log_success(f"\n{scraped_bet_types.get_dataframe()}\n")

    
#     <--------- Race Results ----------- >

    # Get Race Results
    scraped_race_res = ScrapeRaceResult(
        scrape_data = scrape_data,
        race_df=scraped_races.get_dataframe(),
        track_df=scraped_tracks.get_dataframe(),
        race_id='race_id',
        fin_place='fin_place',
        pgm='pgm',
        ml='Morning_Line',
        id_field='id'
    )
    horses_db_records = pd.DataFrame(columns=['name', 'sire'])
    jockey_db_records = pd.DataFrame(columns=['name', 'id'])
    trainer_db_records = pd.DataFrame(columns=['name', 'id'])
    race_res_db_records = pd.DataFrame(columns=['id', 'race_id'])

    if not scraped_race_res.get_dataframe().empty:

        horses_db_records = query_horse_db(horses_db_records, scraped_race_res, db_engine)
        jockey_db_records = query_jockey_db(jockey_db_records, scraped_race_res, db_engine)
        trainer_db_records = query_trainer_db(trainer_db_records, scraped_race_res, db_engine)

        # Associate Horse Names with Database IDs
        scraped_race_res.attach_fk_ids(horses_db_records, trainer_db_records, jockey_db_records)
        scraped_race_res.normalize_fields()

        log_blue(f"\n{horses_db_records.sort_values(by='name')}")
        log_success(f"\n{scraped_race_res.get_dataframe()[['horse_name', 'horse_id']].sort_values(by='horse_name')}")

        # Query DB for race results
        if not scraped_race_res.get_dataframe().empty:
            query, params = build_race_results_query(scraped_race_res.get_dataframe())
            log_debug(f'\n\n{query}\n{params}\n')

            try:
                race_res_db_records = pd.read_sql(sql=query, con=db_engine, params=params, coerce_float=False)
            except Exception as e:
                log_error(f'Failed to read database race results: {e}')
                exit('Aborting Sync')

            if not race_res_db_records.empty:
                scraped_race_res.attach_ids(race_res_db_records)

            log_blue(f'\n{race_res_db_records}')
            log_success(f'\n{scraped_race_res.get_dataframe()}')


    # <--------- Export Report ----------- >

    export_missing_data = {
        "Missing Tracks" : {'table' : 'race_tracks', 'records' : scraped_tracks.get_missing()},
        "Missing Races" :  {'table' : 'races', 'records' : scraped_races.get_missing()},
        "Missing Bet Types" : {'table' : 'mapped_race_bet_types', 'records' : scraped_bet_types.get_missing()},
        "Missing Wager Types" : {'table' : 'res_wps_wager_type', 'records' : scraped_wager_types.get_missing()},
        "Missing Horses" : {'table': 'horses', 'records' : scraped_race_res.get_missing_horses()},
        "Missing Trainers" : {'table': 'trainers', 'records' : scraped_race_res.get_missing_trainers()},
        "Missing Jockeys" : {'table': 'jockeys', 'records' : scraped_race_res.get_missing_jockeys()},
        "Missing Results" : {'table' : 'race_results', 'records' : scraped_race_res.get_missing()
        },
    }

    print_missing_records(export_missing_data)

    merged_races = scraped_races.merge_records(races_db_records)
    merged_race_results = scraped_race_res.merge_records(race_res_db_records)

    # these results will all be empty if all recors match what is in the database, and nothing is missing
    empties = [merged_races.empty, merged_race_results.empty]
    empties += [x['records'].empty for x in export_missing_data.values() if 'records' in x]

    # Only open the writer if there's something to write
    if not all(empties):
        with pd.ExcelWriter(f'{today_label}-HRNation-V3-EXPORTS.xlsx') as writer:
            
            if not merged_races.empty:
                sorted_race_data = merged_races.sort_values(by=['id', 'source'])
                sorted_race_data.to_excel(writer, sheet_name='Race Differences')

            if not merged_race_results.empty:
                sorted_res_data = merged_race_results.sort_values(by=['id', 'source'])
                sorted_res_data.to_excel(writer, sheet_name='Results Differences')

            [export_missing_data[k]['records'].to_excel(writer, sheet_name=k) for k, v in export_missing_data.items() if not export_missing_data[k]['records'].empty]

        if update:
            log_blue('updating fractional times and race statuses')
            scrp_times = scraped_races.get_existing().query('fractional_times != ""')
            if not scrp_times.empty:
                neeeds_update = get_fractional_time_updates(scrp_times, races_db_records)
                if not neeeds_update.empty:
                    updates = build_fractional_time_update(neeeds_update)
                    log_debug(updates)

                    with db_engine.begin() as conn:
                        resp = conn.execute(updates)
                        log_success(f'Updated Fractional Times for completed races!')
                else:
                    log_success('No fraciontal times need be updated')
            else:
                log_debug('No Fractional times found in HRNation')


        log_info('Inserts:' + str(inserts))
        if inserts:
            log_blue('Inserting missing records')
            export_missing_data['Missing Races']['records'].drop(columns='track_name', inplace=True)

            for k, v in export_missing_data.items():

                if k == "Missing Tracks":
                    continue

                if v['records'].empty:
                    log_success(f'No Missing data in table {v["table"]}!')
                    continue

                # log_error(f'Local run: {local_run} : {type(local_run)}')
                if local_run and (str(local_run).lower() == 'true'):
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

    else:
        log_success('Nothing to update! Database is Synced!')
    log_success('Sync Run Complete!')
    print('Sync Complete')


if __name__ == "__main__":
    main(inserts=DB_INSERTS, update=DB_UPDATES, local_run=LOCAL_RUN)