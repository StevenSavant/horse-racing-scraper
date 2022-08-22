from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Track(Base):
   __tablename__ = "race_tracks"

   id = Column(Integer, primary_key=True)
   track_code = Column(String(40))
   track_name = Column(String(40))
   track_name_ha = Column(String(40))

   def __repr__(self):
       return f"Track(id={self.id!r}, track_code={self.track_code!r}, track_name={self.track_name!r}, track_name_ha={self.track_name_ha!r})"


class Races(Base):
   __tablename__ = "races"

   id = Column(Integer, primary_key=True)
   fk_track_id = Column(Integer)
   race_num = Column(String(40))
   race_date = Column(DateTime)
   race_track_surf = Column(String(40))
   purse_usd_size = Column(String(40))
   race_status = Column(String(40))
   race_class = Column(String(255))
   fractional_times = Column(String(255))
   race_track_dist = Column(String(255))
   off_at_time = Column(String(40))
   dt_est_start = Column(DateTime)

   def __repr__(self):
       return f"Track(id={self.id!r}, track_code={self.track_code!r}, track_name={self.track_name!r}, track_name_ha={self.track_name_ha!r})"


class RaceBetTypes(Base):
   __tablename__ = "mapped_race_bet_types"

   id = Column(Integer, primary_key=True)
   fk_race_id = Column(Integer)
   bet_type = Column(String(40))

   def __repr__(self):
       return f"RaceBetTypes(id={self.id!r}, fk_race_id={self.fk_race_id!r}, bet_type={self.bet_type!r})"


class Horses(Base):
   __tablename__ = "horses"

   id = Column(Integer, primary_key=True)
   name = Column(String(255))
   sire = Column(String(255))

   def __repr__(self):
       return f"Horses(id={self.id!r}, name={self.name!r}, age={self.age!r}, gender={self.gender!r},  weight={self.weight!r})"


class Jockeys(Base):
   __tablename__ = "jockeys"

   id = Column(Integer, primary_key=True)
   name = Column(String(255))

   def __repr__(self):
       return f"Jockeys(id={self.id!r}, name={self.name!r})"


class Trainers(Base):
   __tablename__ = "trainers"

   id = Column(Integer, primary_key=True)
   name = Column(String(255))

   def __repr__(self):
       return f"Trainers(id={self.id!r}, name={self.name!r})"



class RaceResults(Base):
   __tablename__ = "race_results"

   id = Column(Integer, primary_key=True)
   race_id = Column(Integer)
   horse_id = Column(String(40))
   jockey_id = Column(DateTime)
   trainer_id = Column(String(40))

   pgm = Column(String(40))
   wps_win = Column(String(255))
   wps_place = Column(String(255))
   wps_show = Column(String(255))
   fin_place = Column(Integer)
   scratched = Column(Integer)
   Morning_Line = Column(String(45))

   def __repr__(self):
       return f"RaceResults(id={self.id!r}, race_id={self.race_id!r}, horse_id={self.horse_id!r})"
       

class OpenRace(Base):
   __tablename__ = "open_races"

   id = Column(Integer, primary_key=True)
   fk_track_id = Column(Integer)
   track_name = Column(String(40))
   track_code = Column(String(40))
   race_status = Column(String(40))
   race_num = Column(String(40))
   dt_est_start = Column(DateTime)

   def __repr__(self):
       return f"OpenRace(race_id={self.id!r}, track_name={self.track_name!r}, race_num={self.race_num!r}, track_code={self.track_code!r})"


class MappedHorseOdds(Base):
   __tablename__ = "mapped_res_horse_odds"

   id = Column(Integer, primary_key=True)
   fk_horse_id = Column(Integer)
   fk_race_id = Column(Integer)
   fk_res_horse_id = Column(Integer)
   odds = Column(String(40))
   race_status = Column(String(40))

   def __repr__(self):
       return f"MappedHorseOdds(fk_race_id={self.fk_race_id!r}, fk_horse_id={self.fk_horse_id!r}, odds={self.odds!r}"