import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
from typing import Dict, Iterable, List
import pytz

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    import pymssql


@dataclass
class Tag:
    id: str
    description: str
    units: str


@dataclass
class TagReading:
    value: float
    timestamp: datetime
    quality: int

    def quality_str(self) -> str:
        """
        Converts the raw quality to a string representation
        :return: a string representation of signal quality
        """
        if self.quality == 192:
            return 'Good'
        elif self.quality == 0:
            return 'Bad'
        else:
            return 'Unknown'

    def __str__(self):
        return f"{self.value:.2f} at {self.timestamp:%m/%d/%y %H:%M:%S %z} (S:{self.quality_str()})"

    def __repr__(self):
        return self.__str__()


class Historian:
    def __init__(self, site_abbreviation: str = None, server: str = None, user: str = None, password: str = None,
                 timezone: str = None, database: str = 'ctc_config'):
        self.server = server if server is not None else os.environ['DATAPARC_SERVER']
        self.user: str = user if user is not None else os.environ['DATAPARC_USERNAME']
        self.password: str = password if password is not None else os.environ['DATAPARC_PASSWORD']
        self.database: str = database
        self.abbreviation = site_abbreviation if site_abbreviation is not None else os.environ['DATAPARC_SITE_ABBREVIATION']
        self.timezone = pytz.timezone(timezone) if timezone is not None else pytz.timezone(os.environ.get('DATAPARC_TIMEZONE', "UTC"))

    def get_all_tags(self) -> List[Tag]:
        """
        Retrieves a list of tag metadata for all of the defined tags in dataparc
        :return: A list of all tags with metadata
        """
        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(f" \
                    SELECT \
                        '{self.abbreviation}.' + ds.name +'.'+ t.sname [Id], \
                        t.lname [Description], \
                        t.units [Units] \
                    FROM ctc_tag t inner JOIN ctc_dssource ds on t.dssourceid = ds.dssourceid")
                results = cursor.fetchall()
                return [Tag(r['Id'], r['Description'], r['Units']) for r in results]

    def get_current_tag_reading(self, tag_id: str, escape_slashes=True):
        """
        Retrieves the current raw reading, if any, for the specified tag_id
        :param tag_id: The tag to retrieve
        :param escape_slashes: specifies whether any '/' need to be replaced with '//'
        :return: The current reading, if any, None otherwise
        """
        if escape_slashes:
            tag_id = tag_id.replace('/', '//')

        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(" \
                    SELECT REPLACE(tagname, '//', '/') [Id], \
                           Timestamp [Timestamp], \
                           value [Value], \
                           quality [Quality] \
                    FROM   [dbo].[Ctc_fn_parcdata_readrawtags] (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, ';')  \
                    WHERE  shistorianquality != 'NoBound'", tag_id)
                results = cursor.fetchall()
                if not results:
                    return None
                else:
                    result = results[-1]
                    return TagReading(result['Value'], self.timezone.localize(result['Timestamp']), result['Quality'])

    def get_current_tags_readings(self, tag_ids: Iterable[str], escape_slash=True) -> Dict[str, TagReading]:
        """
        A method that returns the current raw readings, if any, for each dataparc tag_id
        :param tag_ids:  A list of Dataparc tag identifiers
        :param escape_slash: specifies whether the / needs to be escaped for dataparc queries
        :return: A dictionary containing the current reading for each tag, if any.
        """
        if escape_slash:
            tag_ids = [s.replace('/', '//') for s in tag_ids]
        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(" \
                    SELECT REPLACE(tagname, '//', '/') [Id], \
                       Timestamp [Timestamp], \
                       value [Value], \
                       quality [Quality] \
                    FROM   [dbo].[Ctc_fn_parcdata_readrawtags] (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, ';')  \
                    WHERE  shistorianquality != 'NoBound'", ";".join(tag_ids))
                return {r['Id']: TagReading(r['Value'], self.timezone.localize(r['Timestamp']), r['Quality']) for r in cursor}

    def get_tag_readings(self, tag_id: str, start_time: datetime, end_time: datetime, escape_slashes=True)\
            -> List[TagReading]:
        """
        Retrieves all raw readings between start and end for the specified tag_id, sorted by timestamp ascending
        :param tag_id: The Dataparc tag identifier
        :param start_time: start of the requested time range
        :param end_time: end of the requested time rang
        :param escape_slashes: specifies whether the '/' needs to be replaced with '//'
        :return: The TagReadings sorted chronologically
        """
        if not start_time <= end_time:
            raise ValueError("A valid time range is required.")

        if escape_slashes:
            tag_id = tag_id.replace('/', '//')

        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(" \
                    SELECT REPLACE(tagname, '//', '/') [Id], \
                       Timestamp [Timestamp], \
                       value [Value], \
                       quality [Quality] \
                    FROM   [dbo].[Ctc_fn_parcdata_readrawtags] (%s, %s, %s, 1, ';')  \
                    WHERE  shistorianquality != 'NoBound'",
                               (tag_id, self.timezone.localize(start_time), self.timezone.localize(end_time)))
                results = [TagReading(r['Value'], self.timezone.localize(r['Timestamp']), r['Quality']) for r in cursor]
                return results

    def get_tags_readings(self, tag_ids: Iterable[str], start_time: datetime, end_time: datetime, escape_slash=True)\
            -> Dict[str, List[TagReading]]:
        """
        Retrieves all tag readings within the time range provided, sorted by timestamp ascending for each tag id

        :param tag_ids: list of tag identifiers
        :param start_time: start of requested time range
        :param end_time: end of requested time range
        :param escape_slash: whether a second '/' needs to be added for each '/'
        :return: A dictionary containing the tag readings sorted in chronological order
        """
        if escape_slash:
            tag_ids = [s.replace('/', '//') for s in tag_ids]

        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(" \
                    SELECT REPLACE(tagname, '//', '/') [Id], \
                       Timestamp [Timestamp], \
                       value [Value], \
                       quality [Quality] \
                    FROM   [dbo].[Ctc_fn_parcdata_readrawtags] (%s, %s, %s, 1, ';')  \
                    WHERE  shistorianquality != 'NoBound'", (";".join(tag_ids), self.timezone.localize(start_time), self.timezone.localize(end_time)))
                results = cursor.fetchall()
                result: Dict[str, List[TagReading]] = {}
                for row in results:
                    if row['Id'] not in result:
                        result[row['Id']] = [TagReading(row['Value'], self.timezone.localize(row['Timestamp']), row['Quality'])]
                    else:
                        result[row['Id']].append(TagReading(row['Value'], self.timezone.localize(row['Timestamp']), row['Quality']))
                return result

    def get_tags_readings_interpolated(self, tag_ids: Iterable[str], start_time: datetime, end_time: datetime, step_size=60, aggregate='AVERAGE', escape_slash=True, remove_microseconds=True)\
            -> Dict[str, List[TagReading]]:
        """
        Retrieves all tag readings within the time range provided, sorted by timestamp ascending for each tag id

        :param tag_ids: list of tag identifiers
        :param start_time: start of requested time range
        :param end_time: end of requested time range
        :param escape_slash: whether a second '/' needs to be added for each '/'
        :return: A dictionary containing the tag readings sorted in chronological order
        """
        if escape_slash:
            tag_ids = [s.replace('/', '//') for s in tag_ids]

        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(" \
                    SELECT REPLACE(tagname, '//', '/') [Id], \
                       Timestamp [Timestamp], \
                       value [Value], \
                       quality [Quality] \
                    FROM   [dbo].[Ctc_fn_parcdata_readinterpolatedtags] (%s, %s, %s, %s, %s, ';')  \
                    WHERE  shistorianquality != 'NoBound'", (";".join(tag_ids), self.timezone.localize(start_time), self.timezone.localize(end_time), aggregate, step_size))
                results = cursor.fetchall()
                result = {}
                for row in results:
                    if row['Id'] not in result:
                        result[row['Id']] = [TagReading(row['Value'], self.timezone.localize(row['Timestamp'].replace(microsecond=0)), row['Quality'])]
                    else:
                        result[row['Id']].append(TagReading(row['Value'], self.timezone.localize(row['Timestamp'].replace(microsecond=0)), row['Quality']))
                return result


if __name__ == '__main__':
    import time
    from dotenv import load_dotenv
    import pandas as pd
    from statistics import stdev
    load_dotenv()
    start_time = time.time()
    hist = Historian()
    current_value = hist.get_current_tags_readings(["Example Tag 1", "Example Tag 2"])
    start = datetime(2018, 9, 30) + timedelta(days=1)
    start2 = datetime()
    end = datetime(2018, 7, 19)
    for i in range(30):
        start_date = start + timedelta(days=i)
        end_date = start_date + timedelta(days=1)
        values = hist.get_tags_readings_interpolated(["Example Tag 3", "Example Tag 4"], start_date, end_date, step_size=1, aggregate="INTERPOLATIVE")
        to_consider = []
        bad_seconds = 0
        for i in range(len(values["Example Tag 3"])):
            if values["Example Tag 4"][i].value > 300.0:
                to_consider.append(values["Example Tag 3"][i].value)
            else:
                bad_seconds += 1
        print(f"{stdev(to_consider) if to_consider else 0.0} on {start_date:%m/%d/%y} - ({bad_seconds} seconds excluded)")
    
    # bad_times = {x.timestamp: True for x in list(filter(lambda x: x.value < 300, values["Example Tag 4"]))}
    # print(len(bad_times.keys()), " bad seconds")
    # filtered = [x.value for x in values["Example Tag 3"] if x.timestamp not in bad_times.keys()]# list(filter(lambda x: x.value > 10, values["Example Tag 3"]))
    

    # print(stdev(filtered))
    end_time = time.time()
    print(f"{end_time - start_time} seconds.")
    # values_single = hist.get_tag_readings("Example Tag 5", datetime.now() - timedelta(minutes=1), datetime.now())

    # df = pd.DataFrame({k: {l.timestamp.replace(tzinfo=None): l.value for l in lv} for k, lv in values.items()})
    # print(df.head(10))

