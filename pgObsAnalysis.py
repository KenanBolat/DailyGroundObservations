import datetime


class analyzeObservations (object):
    def __init__(self):
        import psycopg2
        self.starttime = datetime.datetime.now()
        # sck_observation || spa_observation || syn_observation || aws_observation
        self.dbTabName = ''
        self.port = 5433
        self.password = '1'
        self.dbname = 'mgmDb'
        self.user = 'postgres'
        self.conn = psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password, port=self.port)
        self.curr = self.conn.cursor()
        print('Analysis Started : ', self.starttime)

    def getObservationTables(self):
        self.curr.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' and table_name ilike '%%observation';")
        return self.curr.fetchall()

    def locationChangeAnalysis(self, tables):
        for table in tables:
            self.dbTabName = table[0]
            self.curr.execute(
                f'SELECT DISTINCT(stationid) FROM {self.dbTabName}')
            self.uniqueStationId = self.curr.fetchall()
            print('Found ', len(self.uniqueStationId), 'unique stations.')
            stationIds = self.uniqueStationId
            self.uniqueCount = 0
            self.notUniqueList = []
            # Get distinct locations of stations
            for stationid in stationIds:
                self.curr.execute(
                    f'SELECT DISTINCT(st_astext(geom)) FROM {self.dbTabName} WHERE stationid = {stationid[0]}')
                self.locations = self.curr.fetchall()
                if len(self.locations) > 1:
                    self.notUniqueList.append(stationid[0])
                else:
                    self.uniqueCount += 1
            print('Analysis Completed : ', datetime.datetime.now())
            if len(self.notUniqueList) > 1:
                print('Location of ', len(self.notUniqueList),
                      f' stations in table {self.dbTabName} have changed over time.')
            else:
                print(
                    f"Locations of {self.uniqueCount} stations in table {self.dbTabName} haven't changed over time.")
            self.performanceLogger()
        # If a station have two or more individual locations;
        # find when it has been changed last time.

    def performanceLogger(self):
        # Log quantities of fetched records , unique station id's and program execution time to a file.
        logtime = datetime.datetime.now()
        self.curr.execute(f'SELECT COUNT(*) FROM {self.dbTabName}')
        rowQuantityOfTable = self.curr.fetchall()
        filename = f"pgObsAnalysis_{self.dbTabName}__{logtime.strftime('%d_%m_%y_%M%H%S')}.txt"
        with open(filename, 'a+') as file:
            file.write(
                f'Analysis Report for {self.dbTabName} table on {datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")} \n')
            file.write(
                f'Total Rows of the Analyzed Table : {rowQuantityOfTable[0][0]} rows. \n')
            file.write(
                f'Total Unique Stations Analyzed : {len(self.uniqueStationId)} stations. \n')
            file.write(
                f'{len(self.notUniqueList)} of {len(self.uniqueStationId)} stations have changed location over time. \n'
            )
            file.write(
                f'Program Execution Time : {datetime.datetime.now() - self.starttime} seconds.\n'
            )


analiz = analyzeObservations()
obsTables = analiz.getObservationTables()
analiz.locationChangeAnalysis(obsTables)
