import json
import jsonlines
import sys
import os
from collections import Counter
from collections import OrderedDict
from functools import reduce
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import sum as sqlSum
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


class ETLExcerise:

    def __init__(self, arguments):
        # User data from the ETL as a python List
        self.allUsersList = []
        # User data list with the transformed schema
        self.allUsersDataList = []
        # Counts the number of times a word shows up in all posts
        self.allUsersWordCount = Counter()
        # The number of posts made by all users
        self.allUsersPostCount = 0
        # The Spark Context
        self.sc = SparkContext('local', 'Product Sales')
        # The Spark Session
        self.spark = SparkSession.builder.appName('ETL Excerise').getOrCreate()
        # The SQL Context
        self.sqlContext = SQLContext(self.sc)
        # The RDD from the Products File
        self.productFile = None
        # The RDD from the Sales File
        self.salesFile = None

        # Run the code!
        self.processArgs(arguments)
        self.performEtl()
        self.solution1_1()
        self.solution1_2()
        self.solution2_1()

    @staticmethod
    def getMostCommonItems(itemCount, singleReturn=False):
        """!
        Get the items with the highest frequency from a Counter of items (the keys)
        @param itemCount: The Counter holding the occurence for each item
        @param singleReturn: If enabled, return a single item
        @return The item(s) that appear at the highest frequency
        """
        # Find what the highest frequency is
        maxFrequency = itemCount.most_common(1)[0][1]

        # Keep only the items that have a count equal to the highest frequency
        mostFrequentPairs = filter(lambda wordCountPair: wordCountPair[1] == maxFrequency, itemCount.items())
        mostFrequentItems = list(map(lambda pair: pair[0], mostFrequentPairs))
        if singleReturn:
            return mostFrequentItems[0]
        return mostFrequentItems

    @staticmethod
    def getAttrCounter(users, attr):
        """!
        Make a Counter object based on the frequencies of values for a certain attribute
        @param users: The list of users to collect frequency data from
        @param attr: The attribute that is used on the users
        """
        return Counter([user[attr] for user in users])

    @staticmethod
    def getTotalBalanceOfUsers(users):
        """!
        Get the total balance of the given users
        @parma users: the users selected for balance summation
        """
        return reduce(lambda total, user: total + user['balance'], users, 0.0)

    @staticmethod
    def writeObjectsToSolutionsFile(objects, fileName):
        """!
        Writes the objects to the solution file in a readable format
        @param objects: The list of objects to be written
        @param fileName: The name of the solutions file (it uses the current relative path)
        """
        with open(fileName, mode='w') as solutionFile:
            solutionFile.write(json.dumps(objects, indent=4))

    def getLoversOfFruit(self, fruit):
        """!
        Return only the users who has the specific fruits as their favorite
        @param fruit: The fruit used to filter users
        """
        return list(filter(lambda user: user['favorite_fruit'] == fruit, self.allUsersDataList))

    def processArgs(self, args):
        """!
        Use the program arguments to load the data needed for the program
        @param args: The arguments given from the command line (not including the file name)
        """
        assert len(args) == 3, ('usage: python ETLExcerise.py <Path to ETL File> <Path to Products File> '
                                '<Path to Sales file>')
        etlPath = args[0]
        productsPath = args[1]
        salesPath = args[2]

        # Check that each path is valid
        for path, table, extension in [(productsPath, 'Products', '.csv'),
                                       (salesPath, 'Sales', '.csv'), (etlPath, 'ETL', '.jsonl')]:
            assert os.path.exists(path), '{0} File Path ({1}) is invalid'.format(table, path)
            assert path.endswith(extension), '{0} File {1} is not a {2} file'.format(table, path, extension)

        # Each line in the ETL file is its own dictionary, which we add to our parsed list
        with jsonlines.open(etlPath, 'r') as etlFile:
            for etlLine in etlFile:
                self.allUsersList.append(etlLine)
        assert bool(self.allUsersList), 'No data was found in file'

        # Save the files to the Spark Context
        self.productFile = self.sc.textFile(productsPath)
        self.salesFile = self.sc.textFile(salesPath)

    def performEtl(self):
        """!
        Performs operations needed to obtain the output for Solutions 1.1 and 1.2
        """
        # Iterating though each user information dictionary
        for userDict in self.allUsersList:
            # Counts the number of times a word shows up in all of the user's posts
            userWordCount = Counter()

            for post in userDict['posts']:
                # Get the message string of the post, seperate it by the whitespace, and add the words to the counter
                userWordCount.update(post['post'].split())

            # Add the single user count data to the total count data
            self.allUsersWordCount.update(userWordCount)
            self.allUsersPostCount += len(userDict['posts'])

            # Use OrderedDict to preserve order of the keys
            userObject = OrderedDict()
            userObject['full_name'] = '{0} {1}'.format(userDict['name']['first'], userDict['name']['last'])
            userObject['post_count'] = len(userDict['posts'])
            userObject['most_common_word_in_posts'] = self.getMostCommonItems(userWordCount)
            userObject['age'] = userDict['age']
            userObject['is_active'] = userDict['isActive']
            userObject['favorite_fruit'] = userDict['favoriteFruit']

            # Remove the dollar symbol from the balance, seperate by the commas and put the numbers back together
            userObject['balance'] = float(''.join(userDict['balance'][1:].split(',')))

            # Take the user object
            self.allUsersDataList.append(userObject)

        self.writeObjectsToSolutionsFile(self.allUsersDataList, 'solutions1_0.txt')

    def solution1_1(self, includeLowercase=False):
        """!
        `output the items where the user’s name starts with a “J” and save them as your solution to 1.1.`
        @param includeLowercase: If enabled, users who have a lowercase "j" as the first letter of their full name
            will be included in the solution
        """
        firstLetters = ['J']
        if includeLowercase:
            firstLetters.append('j')

        # Remove users who don't have "J" (perhaps also "j") as the first letter of their full name
        usersWithJName = list(filter(lambda userObj: userObj['full_name'][0] in firstLetters, self.allUsersDataList))
        self.writeObjectsToSolutionsFile(usersWithJName, 'solutions1_1.txt')

    def solution1_2(self):
        """!
        `generate an overall summary output of the entire dataset, with the following schema` (datasetStats)
        `Save these summary stats as your solution to 1.2.`
        """

        # Gather data that will be used for the summary
        totalUserCount = len(self.allUsersDataList)
        activeUsers = list(filter(lambda userData: userData['is_active'], self.allUsersDataList))
        strawberryLovers = self.getLoversOfFruit('strawberry')
        appleLovers = self.getLoversOfFruit('apple')
        # All of the ages of the users, including repeating ages
        allAgesDuplicates = sorted(list(map(lambda user: user['age'], self.allUsersDataList)))
        # Repeating ages are removed
        allAgesDistinct = set(allAgesDuplicates)
        # There is someone that has an age in this set that loves apples
        appleLoversAllAges = set([lover['age'] for lover in appleLovers])
        # There is no one that has an age in this list that loves apples
        agesThatAllHateApples = list(allAgesDistinct - appleLoversAllAges)

        # Use OrderedDict to preserve order of the keys
        accountStats = OrderedDict()
        ageStats = OrderedDict()
        fruitStats = OrderedDict()
        datasetStats = OrderedDict()

        accountStats['total'] = self.getTotalBalanceOfUsers(self.allUsersDataList)
        accountStats['mean'] = accountStats['total'] / totalUserCount
        accountStats['active_user_mean'] = self.getTotalBalanceOfUsers(activeUsers) / len(activeUsers)
        accountStats['strawberry_lovers_mean'] = self.getTotalBalanceOfUsers(strawberryLovers)

        ageStats['min'] = allAgesDuplicates[0]
        ageStats['max'] = allAgesDuplicates[-1]
        ageStats['mean'] = sum(allAgesDuplicates) / totalUserCount
        usersOfAboveAvgBalance = list(filter(lambda user: user['balance'] > ageStats['mean'], self.allUsersDataList))

        if totalUserCount % 2:
            ageStats['median'] = (allAgesDuplicates[totalUserCount//2-1] + allAgesDuplicates[totalUserCount//2]) / 2
        else:
            ageStats['median'] = allAgesDuplicates[totalUserCount//2]
        # If there are even users and their median makes a float value, then there might not be any median-aged users
        usersOfMedianAge = list(filter(lambda user: user['age'] == ageStats['median'], self.allUsersDataList))

        # Nothing in the schema mentions about a tie between two ages, so if that happens... *shrugs*
        ageStats['age_with_most_apple_lovers'] = \
            self.getMostCommonItems(Counter([lover['age'] for lover in appleLovers]))[0]
        ageStats['youngest_age_hating_apples'] = min(agesThatAllHateApples)
        ageStats['oldest_age_hating_apples'] = max(agesThatAllHateApples)

        fruitStats['active_users'] = \
            self.getMostCommonItems(self.getAttrCounter(activeUsers, 'favorite_fruit'), singleReturn=True)
        fruitStats['median_age'] = \
            self.getMostCommonItems(self.getAttrCounter(usersOfMedianAge, 'favorite_fruit'), singleReturn=True)
        fruitStats['acct_balance_gt_mean'] = \
            self.getMostCommonItems(self.getAttrCounter(usersOfAboveAvgBalance, 'favorite_fruit'), singleReturn=True)

        datasetStats['total_post_count'] = self.allUsersPostCount
        datasetStats['most_common_word_overall'] = self.getMostCommonItems(self.allUsersWordCount)
        datasetStats['account_balance'] = accountStats
        datasetStats['age'] = ageStats
        datasetStats['favorite_fruit'] = fruitStats

        self.writeObjectsToSolutionsFile(datasetStats, 'solutions1_2.txt')

    def solution2_1(self):
        """!
        Use the given sales and product files to find the three products with the highest revenue,
        the day they sold for the most, and how much they made that day.
        """
        # Parse the products file a DataFrame
        productsRDD = self.productFile.map(lambda line: line.split(',')).map(lambda row: (int(row[0]), row[1]))
        productSchema = StructType([StructField('ProductId', IntegerType(), True),
                                    StructField('ProductName', StringType(), True)])
        productDF = self.sqlContext.createDataFrame(productsRDD, productSchema)
        productDF = productDF.alias('Products')

        # Parse the sales file a DataFrame
        salesRDD = self.salesFile.map(lambda line: line.split(',')).map(lambda row: (row[0], row[1], row[2], row[3]))
        salesDF = salesRDD.map(lambda x: x).toDF(['ProductIdStr', 'SellDateStr', 'SellPriceStr', 'QuantityStr'])
        salesDF = salesDF.withColumn('ProductId', salesDF['ProductIdStr'].cast(IntegerType()))\
            .drop('ProductIdStr')
        salesDF = salesDF.withColumn('SellDate', salesDF['SellDateStr'].cast(DateType()))\
            .drop('SellDateStr')
        salesDF = salesDF.withColumn('SellPrice', salesDF['SellPriceStr'].cast(DecimalType(precision=38, scale=2)))\
            .drop('SellPriceStr')
        salesDF = salesDF.withColumn('Quantity', salesDF['QuantityStr'].cast(IntegerType()))\
            .drop('QuantityStr')

        # Multiply the cost per item with the number of items brought per transaction
        totalSalesDF = salesDF.selectExpr('ProductId', 'SellDate', 'SellPrice * Quantity as TotalPrice')
        totalSale = totalSalesDF.alias('TotalSale')

        # The top 3 products by cumulative revenue (total sales over the product lifetime).
        top3Products = totalSale \
            .groupBy(totalSale.ProductId)\
            .agg(sqlSum('TotalPrice').alias('ProductTotalSale'))\
            .sort('ProductTotalSale', ascending=False)\
            .limit(3)
        topThreeProducts = top3Products.alias('TopThreeProducts')
        topThreeProducts.show()
        topThreeProducts.rdd.saveAsTextFile('solutions2_0')

        # Get all of the sales from the top 3 sold products
        top3ProductSales = totalSale.join(topThreeProducts, totalSale.ProductId == topThreeProducts.ProductId, 'inner')\
            .select('TotalSale.*')

        # Group the sales by the product (id) and the date of transaction, sum up the sales for that product-day
        productsDatesTotal = top3ProductSales.groupBy([top3ProductSales.ProductId, top3ProductSales.SellDate])\
            .agg(sqlSum('TotalPrice').alias('TotalProductSaleOnDate'))

        # Return the best selling dates into a list of Rows
        topProductSellDates = []
        for product in top3Products.select('ProductId').collect():
            # Get the groups for a single product, sort to find the date with best sales for that product
            topSellDate = productsDatesTotal.filter(productsDatesTotal.ProductId == product.ProductId)\
                .sort('TotalProductSaleOnDate', ascending=False).take(1)
            topProductSellDates.extend(topSellDate)

        # Turn the list of Rows into a DataFrame
        topProductSellDatesDF = self.spark.createDataFrame(topProductSellDates, productsDatesTotal.schema)
        topProductDatesDF = topProductSellDatesDF.alias('TopProductDates')

        # Join with the products to replace their Ids with actual names
        topRevenueDateForProduct = topProductDatesDF\
            .join(productDF, topProductDatesDF.ProductId == productDF.ProductId, 'inner')\
            .selectExpr('Products.ProductName as ProductName',
                        'TopProductDates.TotalProductSaleOnDate as CumulativeRevenueAmount',
                        'TopProductDates.SellDate as MostRevenueDate')
        topRevenueDateForProduct.show()

        # Finally save that DataFrame (to a folder, Spark isn't too good with this)
        topRevenueDateForProduct.rdd.saveAsTextFile('solutions2_1')


if __name__ == '__main__':
    ETLExcerise(sys.argv[1:])
