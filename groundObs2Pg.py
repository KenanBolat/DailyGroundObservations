"""
"""
import datetime
import csv
import os
import psycopg2


# TODO  populating the data from 2015-09-30 till 2019-04-14 takes approximately 9 Hour
# TODO increase speed of insert mechanism


class DataMigration(object):

    def __init__(self):
        self.log = []
        self.starttime = datetime.datetime.now()
        self.__process_path = r'E:\DailyGroundObservations'
        self.port = 5432
        self.type_ = None
        self.password = 'kalman'
        self.dbname = 'MGM'  # 'MGM_Updated'
        self.user = 'postgres'
        self.insertSql = None
        self.host = '192.168.0.221'
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port,
            host=self.host)
        self.curr = self.conn.cursor()
        self.log_path = os.path.join(self.__process_path, "logs")
        self.unwanted_tags = ['CCA', 'CAA', 'CAB', 'CCB', 'CCC', 'CCD', 'CCE', 'CCF', 'RAA', 'RRA', 'RRN', 'VVU', 'KKK',
                              'RRB', 'RRR', 'CCX', 'Ã‡Ã‡A', 'CCS', 'CCZ', 'NULL', '']
        print("Program Started : ", self.starttime)

    def process_type(self, type_):
        self.type_ = type_

    @property
    def data_dir(self):
        return self.__process_path

    @data_dir.setter
    def data_dir(self, value):
        self.__process_path = value

    @data_dir.getter
    def data_dir(self):
        return self.__process_path

    def re_arange_data_aws_spa(self, data_):
        re_arranged_list = list(map(lambda x: None if x in self.unwanted_tags else float(x), data_))
        # stationid, altitude, m_date (Year, Moth, Day, H, M ), snow_depth, geom (lat, long)
        data = (
            int(re_arranged_list[0]),  # stationid
            (re_arranged_list[3]),  # altitude
            int(re_arranged_list[4]),  # Year
            int(re_arranged_list[5]),  # Month
            int(re_arranged_list[6]),  # Day
            int(re_arranged_list[7]),  # Hour
            int(re_arranged_list[8]),  # Minute
            (re_arranged_list[9]),  # Snow Depth
            (re_arranged_list[2]),  # Longitude
            (re_arranged_list[1]),  # Latitude
        )
        return data

    def re_arange_data_syn(self, data_):
        re_arranged_list = list(map(lambda x: None if x in self.unwanted_tags else float(x), data_))
        # stationid, altitude, m_date (Year, Moth, Day, H, M ), snow_depth, geom (lat, long)
        data = (
            int(re_arranged_list[0]),  # stationid
            (re_arranged_list[3]),  # altitude
            int(re_arranged_list[4]),  # Year
            int(re_arranged_list[5]),  # Month
            int(re_arranged_list[6]),  # Day
            int(re_arranged_list[7]),  # Hour
            int(0),  # Minute
            (re_arranged_list[9]),  # Snow Depth
            (re_arranged_list[2]),  # Longitude
            (re_arranged_list[1]),  # Latitude
        )
        return data

    def re_arange_data_tmp(self, data_):
        re_arranged_list = list(map(lambda x: None if x in self.unwanted_tags else float(x), data_))
        data = (
            int(re_arranged_list[0]),  # stationid
            (re_arranged_list[3]),  # altitude
            int(re_arranged_list[4]),  # Year
            int(re_arranged_list[5]),  # Month
            int(re_arranged_list[6]),  # Day
            int(re_arranged_list[7]),  # Hour
            int(0),  # Minute
            (re_arranged_list[8]),  # Temperature
            (re_arranged_list[2]),  # Longitude
            (re_arranged_list[1]),  # Latitude
        )
        return data

    @staticmethod
    def write_log(value):
        with open(datetime.datetime.today().strftime("%Y%m%d") + ".csv", "a+") as c_file:
            csv_write = csv.writer(c_file, delimiter=",")
            csv_write.writerow(value)

    @property
    def getavailablesql_for_type(self):
        temp_dict = {
            # 'TMP': " INSERT INTO tmp_raw_observation (stationid, altitude, m_date, temperature, geom) VALUES "
            #        "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
            #        "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            # 'AWS': "INSERT INTO aws_raw_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
            #        "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
            #        "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            # 'SYN': "INSERT INTO syn_raw_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
            #        "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', "
            #        "'YYYY-MM-DD HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            # 'SPA': "INSERT INTO spa_raw_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
            #        "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
            #        "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            # 'SPA_ALL': "INSERT INTO spa_observations_with_all_measurements  "
            #            "( stationid, m_date, snow_depth, snow_percentage_a1, water_percentage_a1, snow_density_a1, "
            #            "swe_a1, snow_percentage_a2, water_percentage_a2, snow_density_a2, swe_a2) VALUES "
            #            "(  %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00','YYYY-MM-DD HH24:MI:SS'), %s,%s,%s,%s,%s,%s, %s,%s,%s)"
            #
            'TMP': " INSERT INTO sck_observation (stationid, altitude, m_date, temperature, geom) VALUES "
                   "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
                   "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'AWS': "INSERT INTO aws_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
                   "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
                   "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'SYN': "INSERT INTO syn_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
                   "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', "
                   "'YYYY-MM-DD HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'SPA': "INSERT INTO spa_observation (stationid, altitude, m_date, snow_depth, geom) VALUES "
                   "(%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'YYYY-MM-DD HH24:MI:SS'), "
                   "%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'SPA_ALL': "INSERT INTO spa_observation "
                       "( stationid, m_date, snow_depth, snow_percentage_a1, water_percentage_a1, snow_density_a1, "
                       "swe_a1, snow_percentage_a2, water_percentage_a2, snow_density_a2, swe_a2) VALUES "
                       "(  %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00','YYYY-MM-DD HH24:MI:SS'), %s,%s,%s,%s,%s,%s, %s,%s,%s)"

        }
        return temp_dict[self.type_]

    def logger(self, log):
        logfile = open(os.path.join(self.__process_path + '{datetime.datetime.now().strftime("%Y%m%d")}log.txt'), 'a+')
        logfile.write(str(log) + '\n')
        logfile.close()

    def read_and_write_files(self, file_tag=""):
        import glob
        data_path = self.__process_path
        # for file_ in glob.glob1(data_path, "*.txt"):
        for file_ in glob.glob1(data_path, "*" + file_tag + "*.txt"):
            values = []
            filename = os.path.join(data_path, file_)
            file_time = datetime.datetime.now()
            try:
                with open(filename, 'r') as csvfile:
                    spamreader = csv.reader(csvfile, delimiter='|')
                    for row in spamreader:
                        if any(row in self.unwanted_tags for row in row[1:3]):
                            self.write_log([file_, row])
                            self.log.append(row)
                            continue
                        if "spa" in file_:
                            self.type_ = "SPA"
                            values.append(self.re_arange_data_aws_spa(row))
                        elif "aws" in file_:
                            self.type_ = "AWS"
                            values.append(self.re_arange_data_aws_spa(row))
                        elif "sck" in file_:
                            self.type_ = "TMP"
                            values.append(self.re_arange_data_tmp(row))
                        elif "sinoptik" in file_:
                            self.type_ = "SYN"
                            values.append(self.re_arange_data_syn(row))
                        # if file_ != '20151222_awskaryuks.txt':
                        # print(filename)

                    # for value in values:
                    #     try:
                    #         self.curr.execute(self.getavailablesql_for_type, value)
                    #         self.conn.commit()
                    #     except:
                    #         log.append(value)
                self.curr.executemany(self.getavailablesql_for_type, values)
                self.conn.commit()
                print(os.path.basename(filename), "\t\t\t\tDuration : ", datetime.datetime.now() - file_time)

                # self.logger(log)

            except Exception as be:
                print(be)
                self.write_log([file_])
                self.conn.commit()
                continue
        print("Total Duration : ", datetime.datetime.now() - self.starttime)
        print(self.log)


if __name__ == '__main__':
    dat = DataMigration()
    initiation_date = "20200131"
    r = datetime.datetime.strptime(initiation_date, "%Y%m%d") - datetime.datetime.today()
    for i in range(abs(r.days)):
        c = datetime.datetime.strptime(initiation_date, "%Y%m%d") + datetime.timedelta(days=i)
        date_ = c.strftime("%Y%m%d")
        dat.read_and_write_files(date_)
