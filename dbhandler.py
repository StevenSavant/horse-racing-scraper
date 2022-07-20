

from utils import log_debug, log_error
from sqlalchemy import create_engine, engine


def get_engine(db_name, db_type, db_address, username, password, port=3306):
    engine = None
    db_url = f"{db_type}+pymysql://{username}:{password}@{db_address}:{port}/{db_name}"
    log_debug(f'Initializing Database Engine, name: {db_name}, type: {db_type}, address: {db_address}')
    try:
        engine = create_engine(db_url)
    except Exception as e:
        log_error(f'Failed to Initialize Database Engine: {e}')
    return engine


def build_tracks_query(tracks_df):
    trk_clmns = list(tracks_df.columns)
    params = r'%s'
    [params := params + ', %s' for i in range(len(tracks_df[trk_clmns[0]]) - 1)]
    query  = 'SELECT ' + ", ".join(trk_clmns) + '\n'
    query += f'FROM zndlabs.race_tracks \n'
    query += f'WHERE {trk_clmns[0]} IN  ({params})'
    return query, tuple(tracks_df[trk_clmns[0]])


def build_races_day_query(races_df, tracks_df, race_day):
    rc_clmns = list(races_df.columns)
    params = r'%s'
    [params := params + ', %s' for i in range(len(tracks_df['id']) - 1)]
    rc_clmns.remove('race_date')
    query  = 'SELECT DATE(race_date) as race_date, ' + ", ".join(rc_clmns) + '\n'
    query += f'FROM zndlabs.races \n'
    query += f'WHERE DATE(race_date) =  \'{race_day}\' AND fk_track_id IN ({params})'
    return query, tuple(tracks_df['id'])


def build_bet_type_query(bet_typs_df):
    rc_clmns = list(bet_typs_df.columns)
    params = r'%s'
    [params := params + ', %s' for i in range(len(bet_typs_df['fk_race_id']) - 1)]
    query  = 'SELECT ' + ", ".join(rc_clmns) + '\n'
    query += f'FROM zndlabs.mapped_race_bet_types \n'
    query += f'WHERE fk_race_id IN ({params})'
    return query, tuple(bet_typs_df['fk_race_id'])


def build_horse_query(race_res_df):
    horse_names = set(race_res_df['horse_name'])
    params = r'%s'
    [params := params + ', %s' for i in range(len(horse_names) - 1)]
    query  = 'SELECT name, id\n'
    query += f'FROM zndlabs.horses \n'
    query += f'WHERE name IN ({params})'
    return query, tuple(horse_names)


def build_race_results_query(race_res_df):
    race_ids = set(race_res_df['race_id'])
    params = r'%s'
    [params := params + ', %s' for i in range(len(race_ids) - 1)]
    query  = 'SELECT id, race_id, horse_id, pgm, fin_place\n'
    query += f'FROM zndlabs.race_results \n'
    query += f'WHERE race_id IN ({params})'
    return query, tuple(race_ids)


