
from glob import escape
from utils import *

class HorseRacingNation():
    """Wrapper for Horse Racing Nation Data Object
    creates clean interface for retreiving race related data
    """
    def __init__(self, race_date, scrape_data):
        self._race_date = race_date
        self._scrape_data = scrape_data
        return None

    def get_tracks(self):
        tracks = []

        for track in self._scrape_data:
            tracks.append({
                'name' : track,
                'raceCount' : len(self._scrape_data[track])
            })
        
        return tracks
    
    def _parse_race_results(self, race_res, also_rans, runners):
        # if 'runners' in race_data and (not race_data['runners'].empty):
        #     race_res = race_data['runners']

        also_rans = also_rans.to_dict()
        for k in sorted(also_rans['Also Rans'].keys()):
            try:
                name = also_rans['Also Rans'][k]
                if name == '':
                    break

                runner = runners.query(f'Horse == "{name}"')
                race_res = race_res.append({
                        'Runner' : name,
                        'Horse Number' : runner.iloc[0]['PP'],
                        'Win' : '-', 
                        'Place' : '-', 
                        'Show' : '-'
                    }, 
                    ignore_index=True
                )
            except Exception as e:
                log_warn(f'error reading also_ran horse: {e}')
                continue

        race_res = race_res.rename(
            {'Runner':'name', 'Horse Number': 'number'},
            axis='columns'
        )
        race_res = self._convert_numbers(race_res.to_dict('records'))

        for i in range(len(race_res)):
            race_res[i]['fin_place'] = (i + 1)
        
        return race_res
    
    def _convert_numbers(self, record_list):
        resp = []
        for x in record_list:
            try:
                x['number'] = round(float(x['number']))
            except:
                continue
            resp.append(x)
        return resp

    def get_race(self, track_name, race_number):
        race = {}
        race_data = self._scrape_data[track_name][race_number]
        est_time = race_data['ap']['Race Time']
        race_datetime = datetime.strptime(self._race_date, "%Y-%m-%d")
        race['raceNumber'] = int(race_number)
        race['runners'] = []
        race['race_date'] = race_datetime
        race['off_at_time_est'] = est_time
        race['estimatedStartTime'] = add_to_datetime(race_datetime, est_time)
        race['status'] = 'OPEN'
        race['distance'] = race_data['ap']['Length']
        race['raceClass'] = race_data['ap']['Race Class']
        race['surface'] = race_data['ap']['Surface']
        race['purse'] = race_data['ap']['Purse']
        race['betTypes'] = []
        race['results'] = []
        
        try:
            # what happens here can be unpredictable
            race['betTypes'] = race_data['pool']['Pool'].tolist()
        except:
            race['betTypes'] = []

        # build runners... (Yes the table names are backwards)
        runners = race_data['race_results'].rename(
            {'Horse':'name', 'PP': 'number', 'Sire' : 'sire', 'Trainer' : 'trainer', 'Jockey' : 'jockey', '#' : 'scratched', 'ML': 'morningLine'},
            axis='columns'
        )
        runners = self._convert_numbers(runners.to_dict('records'))
        race['runners'] = runners

        # Build Race Results
        try:
            race_results = self._parse_race_results(
                race_res=race_data['runners'], 
                also_rans=race_data['also_ran'], 
                runners=race_data['race_results']
            )
            race['results'] = race_results
        except Exception as e:
            if 'NoneType' not in str(e):
                log_warn(f'No Race Results from HRN: {e}')
            pass


        # update status if there are results
        if race['results']:
            race['status'] = 'FINAL'

        try:
            frac_time = race_data['pool'].at[0, 'Fraction time'].split(',')[-1]
            frac_time = str.strip(frac_time)
            if frac_time[0] == ':':
                frac_time = '0' + frac_time
            race['fractional_times'] = frac_time
        except:
            race['fractional_times'] = ''
            pass
    
        return race

