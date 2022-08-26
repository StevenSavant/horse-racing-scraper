from datetime import datetime
from Models import *
from sqlalchemy import create_engine, engine, select
from utils import log_debug, log_error, log_info, log_warn
import pytz

est = pytz.timezone('US/Eastern')
utc = pytz.utc


def get_engine(db_name, db_type, db_address, username, password, port=3306):
    engine = None
    db_url = f"{db_type}+py{db_type}://{username}:{password}@{db_address}:{port}/{db_name}"
    print(f'Initializing Database Engine, name: {db_name}, type: {db_type}, address: {db_address}')
    engine = create_engine(db_url)
    return engine


def get_db_horses(horse_names, session):
    resultsDict = {}
    results = session.query(Horses).filter(Horses.name.in_(horse_names)).all()
    resultsDict = {r.name : r for r in results}
    return resultsDict


def get_db_jockeys(jockey_names, session):
    resultsDict = {}
    results = session.query(Jockeys).filter(Jockeys.name.in_(jockey_names)).all()
    resultsDict = {r.name : r for r in results}
    return resultsDict


def get_db_trainers(trainer_names, session):
    resultsDict = {}
    results = session.query(Trainers).filter(Trainers.name.in_(trainer_names)).all()
    resultsDict = {r.name : r for r in results}
    return resultsDict


def get_db_bet_types(race_id, session):
    resultsDict = {}
    stmt = select(RaceBetTypes).where(RaceBetTypes.fk_race_id == race_id)
    results = session.scalars(stmt).all()
    resultsDict = {r.bet_type : r for r in results}
    return resultsDict
    

def get_db_tracks(track_names, session):
    resultsDict = {}
    results = session.query(Track).filter(Track.track_name.in_(track_names)).all()
    resultsDict = {r.track_name : r for r in results}
    return resultsDict


def get_db_races(track_id, race_date, session):
    resultsDict = {}
    stmt = select(Races).where(Races.race_date == race_date).where(Races.fk_track_id == track_id)
    results = session.scalars(stmt).all()
    resultsDict = {r.race_num : r for r in results}
    return resultsDict


def get_db_race_results(race_id, session):
    resultsDict = None
    stmt = select(RaceResults).where(RaceResults.race_id == race_id)
    results = session.scalars(stmt).all()
    resultsDict = {r.horse_id : r for r in results}
    return resultsDict

def get_open_races(session):
    resultsDict = None
    stmt = select(OpenRace)
    results = session.scalars(stmt).all()
    resultsDict = {r.id : r for r in results}
    return resultsDict


def get_mapped_odds(race_id, session):
    resultsDict = None
    stmt = select(MappedHorseOdds).where(MappedHorseOdds.fk_race_id == race_id)
    results = session.scalars(stmt).all()
    resultsDict = {r.fk_horse_id : r for r in results}
    return resultsDict


def find_missing_tracks(missing_names, track_info, session):
    key_map = {k.lower(): k for k in track_info}

    results = session.query(Track).filter(Track.track_name.in_(missing_names)).all()

    for track in results:
        tkey = key_map[track.track_name.lower()]
        track_info[tkey]['id'] = track
        track.track_name_ha = tkey              # Update the track_name_ha field to improve search later

        if tkey not in missing_names:
            log_debug(f'Found duplicate records for {tkey}')
        else:
            missing_names.remove(tkey)
    
    log_debug(f'Found {len(results)}')
    session.commit()

    # if there are still yet missing names
    if missing_names:
        #   At this point we could try to do more dynamic searches or just insert with the risk of duplicates
        #   It is best to just generate a report, becuase the knowledge that maps existing track names to names in HA
        #   Should be stored/handles somewhere else
        report = ''
        report += "\nThese tracks could not be found in the database:\n"
        [report := report + '\t' + x + '\n' for x in missing_names]
        report += "Please find closest match manually or insert new record for now...\n"
        log_warn(report)
    
    return track_info


def normalize_info(hrn_race, today_label):
    # Handle Time Parsing and Conversion
    ha_time = hrn_race['estimatedStartTime']
    ha_time = ha_time[:ha_time.rfind('.')]
    ha_time = datetime.fromisoformat(ha_time)
    start_time_utc = ha_time.replace(tzinfo = utc)
    start_time_etc = start_time_utc.astimezone(est)
    race_date = datetime.strptime(today_label, '%Y-%m-%d')

    h, m = start_time_etc.hour, start_time_etc.minute
    am_pm = 'AM'
    if start_time_etc.hour >= 12:
        if start_time_etc.hour > 12:
            h = start_time_etc.hour - 12
        am_pm = 'PM'
    
    off_at_time_est = '{:d}:{:02d} {}'.format(h, m, am_pm)

    # Normalize Money format
    purse = '${:,}'.format(int(hrn_race['purse']))
    status = hrn_race['status']

    # Handle Race Status Logic
    if status != 'CANCELED' and hrn_race['results']:
        status = 'FINAL'
    
    return race_date, start_time_etc, purse, status, off_at_time_est


def update_race_record(db_record, hrn_race):
    log_info(f'Updating Race: {db_record.fk_track_id}/{str(db_record.race_date)[:10]}/{db_record.race_num}/')

    if not db_record.race_class:
        db_record.race_class = hrn_race['raceClass']
    db_record.fractional_times = hrn_race['fractional_times']
    db_record.race_status = hrn_race['status']
    return db_record


def build_race_record(track_id, hrn_race, today_label):
    log_info(f'Adding New Race: {track_id}/{today_label}/{hrn_race["raceNumber"]}/')
    new_race = Races(
        fk_track_id = track_id,
        race_num = hrn_race['raceNumber'],
        race_date = hrn_race['race_date'],
        race_track_surf = hrn_race['surface'],
        purse_usd_size = hrn_race['purse'],
        race_status = hrn_race['status'],
        race_class = hrn_race['raceClass'],
        race_track_dist = hrn_race['distance'],
        fractional_times = hrn_race['fractional_times'],
        off_at_time = hrn_race['off_at_time_est'],
        dt_est_start = hrn_race['estimatedStartTime']
    )
    return new_race


def update_horse(db_record, sire):
    db_record.sire = sire
    return db_record


def build_horse_record(runner):
    new_horse = Horses(
        name = runner['name'],
        sire = runner['sire']
    )
    return new_horse


def build_trainer_record(runner):
    new_trainer = Trainers(
        name = runner['trainer']
    )
    return new_trainer


def build_jockey_record(runner):
    new_jockey = Jockeys(
        name = runner['jockey']
    )
    return new_jockey

def build_horse_odds(db_race_id, db_horse_id, result_id, ha_odds, ha_status):
    new_odds = MappedHorseOdds(
        fk_horse_id = db_horse_id,
        fk_race_id=db_race_id,
        fk_res_horse_id=result_id,
        odds=ha_odds,
        race_status=ha_status
    )
    return new_odds


def add_new_bet_type_mapping(race_id, bet_type):
    bet_type = RaceBetTypes(
        fk_race_id = race_id,
        bet_type = bet_type
    )
    return bet_type


def build_race_res_record(race_id, horse_id, jockey_id, trainer_id, runner, race_results=[]):
    log_info(f'Adding New Race Result: {race_id}/{horse_id}/')
    wps_win = ''
    wps_place = ''
    wps_show = ''
    fin_place = None
    scratched = 0

    if race_results:
        for x in race_results:
            if int(x['number']) == int(runner['number']):
                wps_win, wps_place, wps_show = x['Win'], x['Place'], x['Show']
                fin_place = x['fin_place']
                break

    
    if runner['scratched']:
        scratched = 1

    new_race = RaceResults(
        race_id = race_id,
        horse_id = horse_id,
        jockey_id = jockey_id,
        trainer_id = trainer_id,
        pgm = runner['number'],
        fin_place = fin_place,
        wps_win = wps_win,
        wps_place = wps_place,
        wps_show = wps_show,
        scratched = scratched,
        Morning_Line = runner['morningLine']
    )
    return new_race


def update_race_res_record(db_record, jockey_id, trainer_id, runner, race_results=[]):
    log_info(f'Updating Race Result: {db_record.id}/{db_record.horse_id}')
    wps_win = ''
    wps_place = ''
    wps_show = ''
    fin_place = None
    scratched = 0

    if race_results:
        for x in race_results:
            if int(x['number']) == int(runner['number']):
                wps_win, wps_place, wps_show = x['Win'], x['Place'], x['Show']
                fin_place = x['fin_place']
                break
    
    if runner['scratched']:
        scratched = 1

    db_record.jockey_id = jockey_id
    db_record.trainer_id = trainer_id
    db_record.pgm = runner['number']
    db_record.wps_win = wps_win
    db_record.wps_place = wps_place
    db_record.wps_show = wps_show
    db_record.fin_place = fin_place
    db_record.scratched = scratched
    db_record.Morning_Line = runner['morningLine']