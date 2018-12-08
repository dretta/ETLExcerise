usage: python ETLExcerise.py  \<Path to ETL File\> \<Path to Products File\> \<Path to Sales file\>

Description:
In these problems, you will be asked to read, process, and analyze a dataset.

1: Fruit Salad

In this problem you will write code that reads in a data file and produces several outputs as described below. Whenever we ask for a list of “most common” items, you should return a list with a single entry that is the most common item, or a list with multiple items, if there are cases when multiple items share the max frequency. We will be working with a JSON Lines file.


1.1: Extract and Transform

Read in the file located at: simple-etl.jsonl; generate a list of objects, where each object is a transformed version of the corresponding object in the original file. The output schema should be as follows:

    [

        {
    
            'full_name': <string>,                     # full name of this user, i.e. '{first} {last}'
    
            'post_count': <int>,                       # number of posts for this user
    
            'most_common_word_in_posts': [<string>],   # most common word(s) in this user’s posts
    
            'age': <int>,                              # age of this user
    
            'is_active': <boolean>,                    # whether or not this user "isActive"
    
            'favorite_fruit': <string>,                # the "favoriteFruit" of this user
    
            'balance': <float>,                        # decimal version of this user's account balance
    
        },
    
        ...

    ]


From the above list, output the items where the user’s name starts with a “J” and save them as your solution to 1.1.


1.2: Analysis

Using the full list you created in part 1.1, generate an overall summary output of the entire dataset, with the following schema:


    {
    
        'total_post_count': <int>,               # total number of posts in the dataset
    
        'most_common_word_overall': [<string>],  # most common of the most common words in the dataset
    
        'account_balance': {
    
            'total': <float>,                    # sum of all account balances for all users
    
            'mean': <float>,                     # average account balance for all users
    
            'active_user_mean': <float>,         # average account balance for active users
    
            'strawberry_lovers_mean':  <float>,  # average account balance for users who favor strawberries
    
        },
    
        'age': {
    
            'min': <int>,                        # minimum age of all users
    
            'max': <int>,                        # maximum age of all users
    
            'mean': <float>,                     # mean age of all users
    
            'median': <float>,                   # median age of all users
    
            'age_with_most_apple_lovers': <int>, # age with the most users who favor apples
    
            'youngest_age_hating_apples': <int>, # minimum age of the set of users who do not favor apples
    
            'oldest_age_hating_apples': <int>,   # maximum age of the set of users who do not favor apples
    
        },
    
        'favorite_fruit': {
    
            'active_users': [<string>],          # most common favorite fruit(s) for active users
    
            'median_age': [<string>],            # most common favorite fruit(s) for users of the median age
    
            'acct_balance_gt_mean': [<string>],  # most common favorite fruit(s) for users with a balance greater than the mean
    
        }
    
    }


Save these summary stats as your solution to 1.2.

2: Product Sales

In this problem, you will work with columnar data stored in two tables. You can answer the question using whatever method you’d like, but you may consider loading the data and writing a SQL query, Spark query, or pandas query.


We have two CSV files, one that represents products and another that represents sales data for those products. The files have the following schemas:


products.csv:

    product_id         INT
    
    product_name       STRING


sales.csv

    product_id         INT
    
    sale_date          DATE
    
    sale_price         DECIMAL(38, 2)
    
    quantity           INT

2.1: Queries

Given these tables, write queries to determine the following:

* The top 3 products by cumulative revenue (total sales over the product lifetime).
* The date on which each of the top 3 products generated the most revenue.

Your output should be a table or dataframe with three columns: product name, cumulative revenue amount, and date on which the most revenue was generated. Save this table as your solution to 2.1.

---

Implementation: 

Please run the program in the directory you wish to have the solution files written to.

The solution1 files get overwritten with every run, the solution2 directory might be 

difficult to navigate. I wasn't sure if I could simply copy and paste code from online

to get a better print out, so I took whatever worked for me. The tables mentioned in the 

docs should be printed out in the program.