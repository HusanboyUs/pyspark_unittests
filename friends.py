from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import count,expr,col,count_distinct
from unittest import TestCase


class TestFriends(TestCase):
    def setUp(self) -> None:
        self.spark=SparkSession.builder.appName('Test').getOrCreate()
        self.file=self.spark.read.option('header','true').option('inferschema','true').csv('fakefriends-header.csv')
        self.mockData=[
            ('500','Usmonov','20','2'),
            ('501','Anderson','30','2')
        ]

    def test_countRows(self):
        self.assertEqual(len(self.file.columns),4,msg="Counted 4: 'userID', 'name', 'age', 'friends")
        newRows=self.file.withColumn('friendsWithAge',expr('friends+age'))
        self.assertEqual(len(newRows.columns),5,msg="Counted 4: 'userID', 'name', 'age', 'friends,'friendsWithAge")
        self.spark.stop()
    
    def test_columNames(self):
        new_name=self.file.withColumnRenamed('age','ages')
        self.assertEqual(new_name.columns[2],'ages')

    def test_countOutputs(self):
        self.assertEqual(self.file.where('friends ==2').count(),4,msg='Count of people with 2 friends should be 4 in this datframe!')
        new_dataframe=self.spark.createDataFrame(self.mockData)
        combined_df=self.file.union(new_dataframe)
        test_result=combined_df.where("friends == 2").count()
        self.assertEqual(test_result,6)
        
    def test_teensGroupMost(self):
        selectCol=self.file.select('age','friends')
        agg=selectCol.groupBy('age').sum('friends')
        sortAge=agg.orderBy(col('age').asc())
        filterTeen=sortAge.where('age == 18')
        #transfer to RDD
        myRdd=filterTeen.rdd
        result=''
        for element in myRdd.collect():
            result=element[1]
        self.assertEqual(result,2747,msg='The number of friends by 18 year old teens in source')    
            
    def test_countDistinct(self):
        countDistinct=self.file.select(count_distinct('age','friends'))
        result=int(countDistinct.collect()[0][0])
        self.assertEqual(result,498)

if __name__ == '__main__':
    TestFriends.main()