import pandas as pd
import lxml
from functools import reduce
import pickle
import sqlite3


class Dataframes():

  def __init__(self):
    self.cn = "SeaLevel.csv"
    self.hn = "Co2.html"
    self.seal = pd.read_csv(self.cn, skiprows=3)
    self.Co2 = pd.read_html(self.hn, skiprows=3)

  def CSVdfclean(self):
    self.seal = self.seal.fillna(0)
    self.seal['year'] = self.seal['year'].astype('int')
    self.seal = self.seal.drop(columns=["Jason-1", "Jason-2", "Jason-3"])
    for i in self.seal:
      To = self.seal['TOPEX/Poseidon']
    Topex = []
    for i in To:
      Topex.append(i)
    topexl = []
    for i in range(0, 1223, 45):
      t = Topex[i:i + 12]
      t = round(reduce(lambda x, y: x + y, t) / 46, 2)
      topexl.append(t)
    year = []
    for i in range(1992, 2020, 1):
      year.append(i)
    datac = []
    for i in range(0, 27, 1):
      cl = [year[i], topexl[i]]
      datac.append(cl)
    self.seal = pd.DataFrame(datac, columns=["year", 'topex'])

  def htmldfclean(self):
    self.Co2 = self.Co2[0].drop(columns=[1, 2, 4])
    self.Co2 = self.Co2.rename(columns={
      0: 'year',
      3: 'average',
      5: 'trend',
      6: '#days'
    })
    year = []
    for i in range(1959, 2020, 1):
      year.append(i)

    for i in self.Co2:
      A = self.Co2['average']
      T = self.Co2['trend']
      D = self.Co2['#days']
    avg = []
    tre = []
    days = []
    for i in A:
      avg.append(i)
    for i in T:
      tre.append(i)
    for i in D:
      days.append(i)
    avgl = []
    trel = []
    daysl = []
    for i in range(0, 731, 12):
      t = avg[i:i + 12]
      t = round(reduce(lambda x, y: x + y, t) / 12, 2)
      avgl.append(t)
    for i in range(0, 731, 12):
      a = tre[i:i + 12]
      a = round(reduce(lambda x, y: x + y, a) / 12, 2)
      trel.append(a)
    for i in range(0, 731, 12):
      b = days[i:i + 12]
      b = round(reduce(lambda x, y: x + y, b) / 12, 2)
      daysl.append(b)
    datac = []
    for i in range(0, 61, 1):
      cl = [year[i], avgl[i], trel[i], daysl[i]]
      datac.append(cl)
    self.Co2 = pd.DataFrame(datac,
                            columns=["year", 'average', 'trend', 'days'])

  def concat(self):
    combdf = pd.concat([self.Co2, self.seal])
    combdf = combdf.fillna(0)
    return (combdf)

  #def reducer():


data = Dataframes()
data.CSVdfclean()
data.htmldfclean()
df = data.concat()


class Database:

  def __init__(self):
    self.db = sqlite3.connect("SealCo2.db")
    self.query = '''CREATE TABLE Database (year, average,  trend, days,topex)'''
    self.insert_query = "INSERT INTO Database (year, average,  trend, days,topex) VALUES (?, ?, ?, ?,?)"

  def table(self):
    try:
      cursor = self.db.cursor()
      cursor.execute(self.query)
      self.db.commit()
      print("Success")
    except:
      print("table exists")

  def insert(self, df):
    cursor = self.db.cursor()
    for index, row in df.iterrows():
      list = [row.year, row.average, row.trend, row.days, row.topex]
      cursor.execute(self.insert_query, tuple(list))
    self.db.commit()
    cursor.close()

  def deleteRecord(self, id):
    delete_query = "DELETE from Database where id = " + str(id)
    cursor = self.db.cursor()
    cursor.execute(delete_query)
    self.db.commit()
    print("Success")

  def Search(self, value):
    cursor = self.db.cursor()
    search_query = 'SELECT id FROM Database WHERE name == "{0}"'.format(value)
    cursor.execute(search_query)
    result = cursor.fetchall()
    return result

  def Output(self):
    cursor = self.db.cursor()
    cursor.execute("SELECT * FROM Database")

    myresult = cursor.fetchall()

    for x in myresult:
      print(x)


db = Database()
db.table()
db.insert(df)
db.Output()


class Automaticquerybuilder():

  def __init__(self, filename):
    d = open(filename)
    file = []
    self.name = []
    for line in d:
      rline = line.rstrip()
      cline = rline.split(',')
      self.file.append(cline)
    for i in file[0]:
      for l in i:
        self.name.append(l)
    df = pd.DataFrame(file)
    self.table_name = "Database"

  def table_query(self):
    table_query = "CREATE TABLE" + self.table_name + self.names
    return (table_query)

  def insert(self):
    insert_query = """INSERT TABLE""" + self.table_name + tuple(
      self.names) + """'''VALUES(""" + len(self.names) * "?," + ")"
    return insert_query

  def search(self):
    searching = 'SELECT' + self.name[
      0] + 'FROM' + self.table_name + ' WHERE name == "{0}"'.format(
        self.name[0])
    return searching

  def delete(self):
    deleting = "DELETE from" + self.table_name + " where id = " + str(
      self.name[0])
    return deleting


# reduce all the data down to just years using reduce function and create new dataframse
