"""Data base handler module. Builds SQL Queries and initiates sqlalchemy database engine.
"""

from utils import log_debug, log_error
from sqlalchemy import create_engine, engine


def get_engine(db_name, db_type, db_address, username, password, port=3306):
    """Set credentiasl and informaitno for sqlachemy database engine. Credetials are set but the conneciton is lazy loaded.

    :param db_name: database name
    :type db_name: str
    :param db_type: database language
    :type db_type: _type_
    :param db_address: connection address (ip address or 'localhost' for dev)
    :type db_address: _type_
    :param username: database username
    :type username: _type_
    :param password: db password
    :type password: _type_
    :param port: port, defaults to 3306
    :type port: int, optional
    :return: database engine
    :rtype: sqlalchemy.Engine
    """
    engine = None
    db_url = f"{db_type}+pymysql://{username}:{password}@{db_address}:{port}/{db_name}"
    log_debug(f'Initializing Database Engine, name: {db_name}, type: {db_type}, address: {db_address}')
    try:
        engine = create_engine(db_url)
    except Exception as e:
        log_error(f'Failed to Initialize Database Engine: {e}')
    return engine


def build_tracks_query(tracks_df, db_name, table_name, name_field):
    """Build SQL query for race tracks, attempts to get only unique tracks
    """
    trk_clmns = list(tracks_df.columns)
    params = r'%s'
    [params := params + ', %s' for i in range(len(tracks_df[name_field]) - 1)]
    query  = 'SELECT ' + ", ".join(trk_clmns) + '\n'
    query += f'FROM {db_name}.{table_name} \n'
    query += f'WHERE {name_field} IN  ({params})'
    return query, tuple(set(tracks_df[name_field]))


def build_races_query(races_df, tracks_df, race_day, db_name, table_name, race_date_field, fk_track_id_field, track_id):
    tracks = set(tracks_df[track_id])
    rce_clmns = list(races_df.columns)

    params = r'%s'
    [params := params + ', %s' for i in range(len(tracks) - 1)]
    rce_clmns.remove('race_date')
    rce_clmns.remove('track_name')
    query  = f'SELECT DATE({race_date_field}) as {race_date_field}, ' + ", ".join(rce_clmns) + '\n'
    query += f'FROM {db_name}.{table_name} \n'
    query += f'WHERE DATE({race_date_field}) =  \'{race_day}\' AND {fk_track_id_field} IN ({params})'
    return query, tuple(tracks)


def build_bet_type_query(bet_types_df):
    rc_clmns = list(bet_types_df.columns)
    params = r'%s'
    [params := params + ', %s' for i in range(len(bet_types_df['fk_race_id']) - 1)]
    query  = 'SELECT ' + ", ".join(rc_clmns) + '\n'
    query += f'FROM zndlabs.mapped_race_bet_types \n'
    query += f'WHERE fk_race_id IN ({params})'
    return query, tuple(bet_types_df['fk_race_id'])


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


def build_trainer_query(race_res_df):
    trainer_names = set(race_res_df['trainer'])
    params = r'%s'
    [params := params + ', %s' for i in range(len(trainer_names) - 1)]
    query  = 'SELECT name, id\n'
    query += f'FROM zndlabs.trainers \n'
    query += f'WHERE name IN ({params})'
    return query, tuple(trainer_names)


def build_jockey_query(race_res_df):
    jockey_names = set(race_res_df['jockey'])
    params = r'%s'
    [params := params + ', %s' for i in range(len(jockey_names) - 1)]
    query  = 'SELECT name, id\n'
    query += f'FROM zndlabs.jockeys \n'
    query += f'WHERE name IN ({params})'
    return query, tuple(jockey_names)
