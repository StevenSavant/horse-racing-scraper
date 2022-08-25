import os
import sys
from horseracing_scrape import horse_racing_scrape
from utils import log_blue, log_info, log_warn, set_log_level, log_debug, log_error, log_success

from horseracingnation import HorseRacingNation
from Models import *
from datetime import date
from db_utils import *
from sqlalchemy.orm import sessionmaker


DB_USERNAME = os.getenv("DB_USERNAME", None)
DB_PASSWORD = os.getenv("DB_PASSWORD", None)
DB_ADDRESS = os.getenv("DB_ADDRESS", None)
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME", None)
DB_TYPE = os.getenv("DB_TYPE", None)
DB_UPDATES = os.getenv("DB_UPDATES", True)
DB_INSERTS = os.getenv("DB_INSERTS", True)
DEBUG = os.getenv("DEBUG", True)

today = date.today()
today_label = today.strftime("%Y-%m-%d")
Sessions = None
set_log_level('DEBUG')

try:
    db_engine = get_engine(DB_NAME, DB_TYPE, DB_ADDRESS, DB_USERNAME, DB_PASSWORD, DB_PORT)
    Sessions = sessionmaker(bind=db_engine)
except:
    print('DB Engine failed, Check your environment variables')
    exit()


def compare_race_count(db_races, track_name, count):
    if len(db_races) == count:
        return 'synced'
    elif len(db_races) > count:
        return 'duplicates'
    else:
        return 'missing'


def extract_dependencies(hrn_race):
    horses, jockeys, trainers = [], [], []
    horses_odds = {}

    for runner in hrn_race['runners']:
        horses.append(runner['name'])
        horses_odds[runner['name']] = ""
        jockeys.append(runner['trainer'])
        trainers.append(runner['jockey'])
    
    return horses, horses_odds, jockeys, trainers

def sync_tracks(track_info={}):
    name_list = list(track_info.keys())
    with Sessions() as session:
        try:
            db_tracks = get_db_tracks(name_list, session=session)

            for track in track_info:
                if track in db_tracks:
                    track_info[track]['id'] = db_tracks[track].id
                    name_list.remove(track)
                else:
                    track_info[track]['id'] = None

            if name_list:
                log_warn(f'These Tracks could not be found in the database: {name_list}')

        except Exception as e:
            log_error(f'Critical Error Syncing Tracks: {e}')
    
    return track_info


def sync_track_races_today(hrn, track_name, track_id, race_count):
    hrn_race_cache = {}
    with Sessions() as session:
        try:

            db_races = get_db_races(track_id, today_label, session=session)
            db_state = compare_race_count(db_races, track_name, race_count)
            log_info(f'Today\'s races for {track_name}, are in a state of: {db_state}')
        
            for r in range(1, race_count + 1):             
                if r == 11:
                    print('track')

                hrn_race = hrn.get_race(track_name, str(r))
                    
                
                # Sync Race
                if str(r) in db_races:
                    db_race = db_races[str(r)]
                    update_race_record(db_race, hrn_race)
                else:
                    db_race = build_race_record(track_id, hrn_race, today_label)

                if session.dirty:
                    log_debug(f'race: {db_race.id} Commiting Updates to race')
                
                session.add(db_race)
                session.commit()

                # Collect Dependencies
                log_debug(f'race: {db_race.id} building race dependencies')
                ha_horses, ha_odds, ha_trainers, ha_jockeys = extract_dependencies(hrn_race)
                race_bet_types = get_db_bet_types(db_race.id, session=session)
                db_horses = get_db_horses(ha_horses, session=session)
                db_trainers = get_db_trainers(ha_trainers, session=session)
                db_jockeys = get_db_horses(ha_jockeys, session=session)
                mapped_odds = get_mapped_odds(db_race.id, session=session)
                db_race_results = get_db_race_results(db_race.id, session=session)

                log_debug(f'Checking Bet Types for {db_race.id}')
                if len(hrn_race['betTypesAvailable']) > len(race_bet_types):
                    log_debug('Bet Types out of sync')
                    for x in hrn_race['betTypesAvailable']:
                        if x not in race_bet_types:
                            new_mapping = add_new_bet_type_mapping(db_race.id, x)
                            session.add(new_mapping)
                else:
                    log_debug('Bet Types up to date')

                
                for runner in hrn_race['runners']:

                    if runner['name'] in db_horses:
                        horse_record = db_horses[runner['name']]
                        horse_record.sire = runner['sire']
                    else:
                        horse_record = build_horse_record(runner)

                    if runner['trainer'] in db_trainers:
                        trainer_record = db_trainers[runner['trainer']]
                    else:
                        trainer_record = build_trainer_record(runner)

                    if runner['jockey'] in db_jockeys:
                        jockey_record = db_jockeys[runner['jockey']]
                    else:
                        jockey_record = build_jockey_record(runner)
                    
                    session.add_all([horse_record, trainer_record, jockey_record])
                    session.commit()

                    res_record =  None
                    if horse_record.id in db_race_results:
                        res_record = db_race_results[horse_record.id]

                    if hrn_race['results']:
                        if res_record:
                            update_race_res_record(
                                db_record=res_record,
                                jockey_id = jockey_record.id,
                                trainer_id = trainer_record.id,
                                runner = runner, 
                                race_results =  hrn_race['results']
                            )
                    
                    if not res_record:
                        res_record = build_race_res_record(
                            race_id=db_race.id,
                            horse_id = horse_record.id,
                            jockey_id = jockey_record.id, 
                            trainer_id = trainer_record.id, 
                            runner = runner, 
                            race_results =  hrn_race['results']
                        )
                        session.add(res_record)

                    if horse_record.id in mapped_odds:
                        log_debug('odds already exist, skipping')
                        pass
                    else:
                        log_debug(f'adding new odds for horse: {horse_record.id}')
                        odds_record = build_horse_odds(
                            db_horse_id=horse_record.id,
                            db_race_id=db_race.id,
                            result_id=res_record.id,
                            ha_odds='',
                            ha_status=hrn_race['status']
                        )
                        session.add(odds_record)

                    session.commit()
   
        except Exception as e:
            log_error(f'Critical Error: {e}')

    return hrn_race_cache

def main():
    log_warn(f"RUNNING IN DEBUG: {DEBUG}")
    scrape_data = horse_racing_scrape([today_label], debug=DEBUG)
    log_info(f'Scrapping complete: {list(scrape_data.keys())}')

    hrn = HorseRacingNation(today_label, scrape_data[today_label])
    hrn_tracks = hrn.get_tracks()

     # <---- Process Tracks ---->
    log_info('Processing Race Tracks')
    track_info = { x['name'] : {'race_count' : x['raceCount'] }  for x in hrn_tracks}
    sync_tracks(track_info)
    log_success('Races Tracks Processed!')


    log_info('Processing Races')
    # <----- Process Races ------------>
    all_races = []
    for track in track_info:
        if not track_info[track]['id']:
            log_error(f'track missing from Databse: {track}')
            continue

        if 'Camarero' in track:
            log_debug('skipping Camarero, page data in inconsistent')
            continue

        t_name = track
        t_id = track_info[track]['id']
        t_count = track_info[track]['race_count']
        log_blue(f'Syncing Traces for Track : {t_name} : Races :{t_count}')
        track_races = sync_track_races_today(hrn, t_name, t_id, t_count)
        all_races.append(track_races)

    log_success('Races Processed')
    print('Run Complete')

def test_main():
    scrape_data = horse_racing_scrape([today_label], debug=True)
    hrn = HorseRacingNation(today_label, scrape_data[today_label])
    hrn_tracks = hrn.get_tracks()
    race_1 = hrn.get_race(hrn_tracks[0]['name'], str(1))
    print(hrn_tracks)
    print(race_1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'Prod' :
        DEBUG = False
    main()