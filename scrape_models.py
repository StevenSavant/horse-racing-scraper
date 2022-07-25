from unittest import runner
import pandas as pd
from utils import *

class ScrapeTable:
    """Wrapper for Pandas Dataframe, common to each table
    """
    def __init__(self):
        self._records = None
        pass

    def get_existing(self):
        return self._records.query(f'id != ""')
    
    def get_missing(self):
        return self._records.query(f'id == ""')

    def get_dataframe(self):
        return self._records

    def _add_source(self):
        self._records['Source'] = 'HRNation'


class ScrapeTracks(ScrapeTable):
    """Used to translate Datafraome from Webscraper to a Pandas Dataframe that matches the database
    """
    def __init__(self, scrape_data=None, id_field=None, name_field=None):
        self._ids = id_field
        self._names = name_field
        self._records = None
        if scrape_data:
            self._build_from_scrap_object(scrape_data)
        return None

    def _build_from_scrap_object(self, scrape_data):
        """Parse the Horseracingnation output object to build database mapped dataframe with ids initialized to empty strings

        :param scrape_data: output of horseracingnation scraper
        :type scrape_data: dict
        """
        df = pd.DataFrame( columns=[self._ids , self._names])
        for day in scrape_data:
            for track_name in scrape_data[day]:
                record = {x : track_name if x == self._names else '' for x in list(df.columns)}
                df.loc[track_name] = record
        
        self._records = df

    def _append_track_ids(self, row, db_records):
        u_row = row.copy()
        track_name = u_row[self._names]
        try:
            u_row[self._ids] = db_records.loc[track_name][self._ids]
        except Exception as e:
            u_row[self._ids] = ''
            log_debug(f'Could not to match track {track_name} with a database id: {e}')
        
        return u_row

    def get_track_id(self, track_name):
        """Get the database id of a track, will be '' if the track was not in the database

        :param track_name: Name of the track
        :type track_name: str
        :return: id of the given track or '' if it is not in the database
        :rtype: str
        """
        return self._records.loc[track_name, 'id']

    def attach_ids(self, db_records):
        """Uses database table to find associated records and appends thier id's if they exist

        :param database_df: database dataframe as retured from the pd.read_sql()
        :type database_df: pandads.Dataframe
        """
        db_records.set_index(self._names, inplace=True)
        self._records = self._records.apply(self._append_track_ids, axis=1, db_records=db_records)


class ScrapeRaces(ScrapeTable):
    def __init__(self, scrape_data=None, tracks=None, number_field=None, date_filed=None, sts_field=None, dist_field=None, id_field='id'):
        self._ids = id_field
        self._number = number_field
        self._dates = date_filed
        self._records = None
        self._build_from_scrape_object(scrape_data, tracks)
        return None

    def _build_from_scrape_object(self, scrape_data: dict, tracks: ScrapeTracks):
        """Parse the Horseracingnation output object to build database mapped dataframe with ids initialized to empty strings

        :param scrape_data: output of horseracingnation scraper
        :type scrape_data: dict
        """
        df = pd.DataFrame( columns=[self._ids, self._number])
        for day in scrape_data:
            for track_name in scrape_data[day]:
                for race_num in scrape_data[day][track_name]:
                    
                    if race_num == 'id':
                        continue
                    
                    track_id = tracks.get_track_id(track_name=track_name)           # this way, we don't need the column name
                    if not track_id or track_id == '':                              # Skip Tracks that are not in the database
                        break

                    try:
                        record = {x : '' for x in df.columns}
                        record['race_date'] = day
                        record['track_name'] = track_name                           # Adds Track name for convenience
                        record['fk_track_id'] = track_id
                        record['race_num'] = race_num
                        record['off_at_time'] = scrape_data[day][track_name][race_num]['ap']['Race Time']
                        record['race_track_dist'] = scrape_data[day][track_name][race_num]['ap']['Length']
                        record['race_track_surf'] = scrape_data[day][track_name][race_num]['ap']['Surface']
                        record['race_class'] = scrape_data[day][track_name][race_num]['ap']['Race Class']
                        record['race_sex'] = scrape_data[day][track_name][race_num]['ap']['Sex']
                        record['purse_usd_size'] = scrape_data[day][track_name][race_num]['ap']['Purse']
                        df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
                    except Exception as e:
                        log_info(f'Error Reading Track/Race: {track_name}/{race_num} with error: {e}')

        
        self._records = df

    def _append_race_ids(self, row, db_records):
        u_row = row.copy()
        x, y, z = u_row['race_date'], u_row['fk_track_id'], u_row['race_num']
        try:
            result = db_records.query(f"fk_track_id == {y} & race_num == '{z}'", engine='python')
            if not result.empty:
                u_row['id'] = result.iloc[0]['id']
        except Exception as e:
            log_error(f'failed to match race for date: {x}, on track: {y}, race number: {z} : {e}')
        return u_row


    def attach_ids(self, database_df):
        """Uses database table to find associated records and appends thier id's if they exist

        :param database_df: database dataframe as retured from the pd.read_sql()
        :type database_df: pandads.Dataframe
        """
        self._records = self._records.apply(self._append_race_ids, axis=1, db_records=database_df) 


    def existing(self, track_name, race_number):
        """Check if a given track name and race_number, exist in this table

        :param track_name: the name of the race track
        :type track_name: str
        :param race_number: the race number for this track
        :type race_number: str
        :return: weather or not the track was found
        :rtype: bool
        """
        result = self._records.query(f'track_name == "{track_name}" & {self._number} == "{race_number}"')
        return result.empty

    def merge_records(self, db_records):
        """Merges this scrape table with the coresponding database records (in memory) and return a Dataframe with rows where there are field
        descrepencies. This will be empty if all records match what is in the database.

        :param db_records: dataframe of database table
        :type db_records: pandas.Dataframe
        :return: Resulting Dataframe from comparison
        :rtype: pandas.Dataframe
        """
        sync_cols = ['source', 'id', 'fk_track_id', 'race_num', 'race_class', 'race_sex', 'off_at_time', 'race_track_surf', 'purse_usd_size']
        merged = self._records.copy()
        merged['source'] = 'HRNation'
        db_records['source'] = 'zndlabsDB'

        merged = pd.concat([merged[sync_cols], db_records[sync_cols]], ignore_index=True)
        merged = merged.query('id != ""') # remove missing

        if merged.empty:
            return merged
        
        merged = merged[merged.groupby('id').id.transform('count') > 1]

        # At random times... the Race Number may not be a race number
        merged = merged[merged.id.apply(lambda x: str(x).isnumeric())]
        merged = merged[merged.race_num.apply(lambda x: str(x).isnumeric())]
        merged['id'], merged['race_num']  = merged['id'].astype(int), merged['race_num'].astype(int)
        merged.drop_duplicates(subset=sync_cols[1:], keep=False, inplace=True)

        return merged


class ScrapeBetTypes(ScrapeTable):
    def __init__(self, scrape_data=None, fk_race_id=None, bet_type_field=None, race_df=None, track_df=None, id_field='id'):
        self._ids = id_field
        self._race_ids = fk_race_id
        self._bet_type = bet_type_field
        self._records = None
        self._build_from_scrap_object(scrape_data, race_df, track_df)
        return None

    def _build_from_scrap_object(self, scrape_data, race_df, track_df):
        """Parse the Horseracingnation output object to build database mapped dataframe with ids initialized to empty strings

        :param scrape_data: output of horseracingnation scraper
        :type scrape_data: dict
        """
        df = pd.DataFrame( columns=[self._ids, self._race_ids, self._bet_type])
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
                    
                    try:
                        for v in bet_types['Pool'].values():
                            record = {x : '' for x in df.columns}
                            record['fk_race_id'] = race_row.iloc[0]['id']
                            record['bet_type'] = v.upper()
                            df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
                    except Exception as e:
                        log_debug(f'Could not parse pool info for: {track_name} : {race_num}, May not be available: {e}')
        
        self._records = df


    def _append_bet_type_ids(self, row, db_records):
        u_row = row.copy()
        y, z = u_row['fk_race_id'], u_row['bet_type']
        try:
            result =db_records.query(f"fk_race_id == {y} & bet_type == '{z}'", engine='python')
            if not result.empty:
                u_row['id'] = result.iloc[0]['id']
                return u_row
            return u_row
        except Exception as e:
            log_error(f'failed to match bet type for pair: {y}, {z} : {e}')


    def attach_ids(self, database_df):
        """Uses database table to find associated records and appends thier id's if they exist

        :param database_df: database dataframe as retured from the pd.read_sql()
        :type database_df: pandads.Dataframe
        """
        database_df.reset_index(inplace=True)
        self._records = self._records.apply(self._append_bet_type_ids, axis=1, db_records=database_df)


class ScrapeRaceResult(ScrapeTable):
    def __init__(self,  scrape_data=None, race_df=None, track_df=None, 
                        race_id=None, pgm=None, fin_place=None, id_field='id'):
        self._ids = id_field
        self._race_id = race_id
        self._horse_name = 'horse_name'     # will add horse name to this dataframe
        self._pgm = pgm
        self._fin_place = fin_place
        self._records = None
        self._missing_horses = None
        self._missing_trainers = None
        self._missing_jockeys = None
        self._build_from_scrap_object(scrape_data, race_df, track_df)
        return None

    def _build_from_scrap_object(self, scrape_data, race_df, track_df):
        """Parse the Horseracingnation output object to build database mapped dataframe with ids initialized to empty strings

        :param scrape_data: output of horseracingnation scraper
        :type scrape_data: dict
        """
        df = pd.DataFrame( columns=[self._race_id, self._horse_name, self._fin_place, self._pgm, self._ids])
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
                        # the `#` coloumn is scratched
                        runners = scrape_data[day][track_name][race_num]['runners']
                        race_res = scrape_data[day][track_name][race_num]['race_results'][['Horse', 'Sire', 'Trainer', 'Jockey', '#']]
                        also_ran = scrape_data[day][track_name][race_num]['also_ran'].to_dict()

                        for k in sorted(also_ran['Also Rans'].keys()):
                            try:
                                name = also_ran['Also Rans'][k]

                                if name == '':
                                    break
                            
                                horse_res = scrape_data[day][track_name][race_num]['race_results'].query(f'Horse == "{name}"')
                                runners = runners.append({
                                        'Runner' : name,
                                        'Horse Number' : horse_res.iloc[0]['PP'],
                                        'Win' : '-', 
                                        'Place' : '-', 
                                        'Show' : '-',
                                    }, 
                                    ignore_index=True
                                )
                            except Exception as e:
                                # This should never happend, but if it does... it's not a show stopper
                                log_warn(f'error reading also_ran horse: {e}')
                                continue


                        runners = runners.rename({'Runner':'horse_name', 'Horse Number' : 'pgm', 'Win' : 'wps_win', 'Place' : 'wps_place', 'Show' : 'wps_show'}, axis='columns')
                        race_res = race_res.rename({'Horse':'horse_name', 'Sire' : 'sire', 'Trainer' : 'trainer', 'Jockey' : 'jockey', '#' : 'scratched'}, axis='columns')
                        runners =runners.merge(race_res, on='horse_name')

                        runners['pgm'] = runners['pgm'].astype(int)
                        runners['pgm'] = runners['pgm'].astype(str)
                        runners.index += 1
                        runners.index.name = 'fin_place'
                        runners.reset_index(inplace=True)
                        runners['id'] = ''
                        runners['race_id'] = race_row.iloc[0]['id']
                        runners['horse_id'] = ''
                        runners['jockey_id'] = ''
                        runners['trainer_id'] = ''
                        df = pd.concat([df, runners], ignore_index=True)
                    except Exception as e:
                        continue

        self._records = df 

    def _append_forign_ids(self, row, horse_db_records, trainer_db_records, jockey_db_records):
        xrow = row.copy()
        x = xrow['horse_name'].lower()
        y = xrow['trainer'].lower()
        z = xrow['jockey'].lower()
        try:
            result = horse_db_records.query(f'name == "{x}"', engine='python')
            result1 = trainer_db_records.query(f'name == "{y}"', engine='python')
            result2 = jockey_db_records.query(f'name == "{z}"', engine='python')

            if not result.empty:
                log_debug(f'Associating id with horse: {x}')
                xrow['horse_id'] = result.loc[result.index[0], 'id']
            else:
                log_debug(f'could not find match for horse name: {x}')
                xrow['horse_id'] = 0

            if not result1.empty:
                log_debug(f'Associating id with trainer: {y}')
                xrow['trainer_id'] = result1.loc[result1.index[0], 'id']
            else:
                log_debug(f'could not find match for trainer name: {y}')
                xrow['trainer_id'] = 0

            if not result2.empty:
                log_debug(f'Associating id with jockey: {z}')
                xrow['jockey_id'] = result2.loc[result2.index[0], 'id']
            else:
                log_debug(f'could not find match for jockey name: {z}')
                xrow['jockey_id'] = 0
            
            return xrow
        except Exception as e:
            log_error(f'failed to match any foreign (horse/jockey/trainer) keys name for horse: {x} to ids: {e}')
    

    def get_missing_horses(self):
        self._missing_horses = self._missing_horses.rename({'horse_name' : 'name'}, axis='columns')
        self._missing_horses['id'] = ''
        self._missing_horses.drop_duplicates(keep='first', inplace=True)
        return self._missing_horses

    def get_missing_trainers(self):
        self._missing_trainers = self._missing_trainers.rename({'trainer' : 'name'}, axis='columns')
        self._missing_trainers['id'] = ''
        self._missing_trainers.drop_duplicates(keep='first', inplace=True)
        return self._missing_trainers

    def get_missing_jockeys(self):
        self._missing_jockeys = self._missing_jockeys.rename({'jockey' : 'name'}, axis='columns')
        self._missing_jockeys['id'] = ''
        self._missing_jockeys.drop_duplicates(keep='first', inplace=True)
        return self._missing_jockeys

    def attach_fk_ids(self, horse_df, train_df, jock_df):
        """Uses horse_df table to find associated horse, jockey, and trainer records and appends thier id's if they exist. This is needed for the race results.
        :param horse_df: database dataframe as retured from the pd.read_sql()
        :type horse_df: pandads.Dataframe
        """
        self._records = self._records.apply(
            self._append_forign_ids, 
            axis=1,
            horse_db_records=horse_df,
            trainer_db_records=train_df,
            jockey_db_records=jock_df,
        )


    def _append_race_res_ids(self, row, db_records):
        u_row = row.copy()
        y, z = u_row['race_id'], u_row['horse_id']
        try:
            result =db_records.query(f"race_id == {y} & horse_id == {z}", engine='python')
            if not result.empty:
                u_row['id'] = result.iloc[0]['id']
                return u_row
            return u_row
        except Exception as e:
            log_error(f'failed to append race result ids for race: {y}, horse {z}, horse name: {row["horse_name"]} with error: {e}')
    

    def attach_ids(self, database_df):
        """Uses database table to find associated horse records and appends thier id's if they exist. This is needed for the race results.

        :param database_df: database dataframe as retured from the pd.read_sql()
        :type database_df: pandads.Dataframe
        """
        self._records = self._records.apply(self._append_race_res_ids, axis=1, db_records=database_df)


    def normalize_fields(self):
        """Convert field data formats to those matching what is in the database. Removes incomplete data (missing horses and missing races)
        """
        # Format records to match database
        self._records['race_id'] = self._records['race_id'].fillna(0)
        self._records['race_id'] = self._records['race_id'].astype(int)
        self._records['horse_id'] = self._records['horse_id'].fillna(0)
        self._records['horse_id'] = self._records['horse_id'].astype(int)
        self._records['trainer_id'] = self._records['trainer_id'].fillna(0)
        self._records['trainer_id'] = self._records['trainer_id'].astype(int)
        self._records['jockey_id'] = self._records['jockey_id'].fillna(0)
        self._records['jockey_id'] = self._records['jockey_id'].astype(int)
        self._records['pgm'] = self._records['pgm'].fillna(0)
        self._records['pgm'] = self._records['pgm'].astype(str)
        self._records['fin_place'] = self._records['fin_place'].fillna(0)
        self._records['fin_place'] = self._records['fin_place'].astype(int)

        # Drops records with missing horses or missing Races
        self._missing_horses = self._records.query('horse_id == 0')[['horse_name', 'sire']].copy()
        self._missing_trainers = self._records.query('trainer_id == 0')[['trainer']].copy()
        self._missing_jockeys = self._records.query('jockey_id == 0')[['jockey']].copy()
        self._records = self._records[self._records.horse_id != 0]
        self._records = self._records[self._records.trainer_id != 0]
        self._records = self._records[self._records.jockey_id != 0]

    def merge_records(self, db_records):
        """Merges this scrape table with the coresponding database records (in memory) and return a Dataframe with rows where there are field
        descrepencies. This will be empty if all records match what is in the database.

        :param db_records: dataframe of database table
        :type db_records: pandas.Dataframe
        :return: Resulting Dataframe from comparison
        :rtype: pandas.Dataframe
        """
        # Merge Race Results
        if self._records.empty:
            return self._records
        
        sync_cols = ['source', 'id', 'race_id', 'horse_id', 'pgm', 'fin_place']
        merged = self._records.copy()
        merged['source'] = 'HRNation'
        db_records['source'] = 'zndlabsDB'

        merged = pd.concat([merged[sync_cols], db_records[sync_cols]], ignore_index=True)
        merged = merged.query('id != ""') # remove missing

        if merged.empty:
            return merged

        merged = merged[merged.groupby('id').id.transform('count') > 1]
        merged['id'], merged['horse_id'] = merged['id'].astype(int), merged['horse_id'].astype(int)
        merged.drop_duplicates(subset=sync_cols[1:], keep=False, inplace=True)

        return merged