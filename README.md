# Final Project for CS6240 Spring2024

## Title 
<h3>Identify Similar Restaurants in Yelp Dataset by Utilizing Frequent Itemset Mining</h3>

## Team Member
Zhufeng Qiu, Peng Zhang 

## Problem Statement
How to identify state-specific similar restaurants when given users’ review data and restaurant list

## Introduction to the Problem
A restaurant owner or a food delivery platform always wants to know which restaurants are preferred by a user so that they can make better decisions and achieve better economic effects.
To solve this problem, our group wants to find similar restaurants based on user preferences. We stipulate that if some restaurant combinations are reviewed by different users at the same time, then we say that the restaurants in these restaurant combinations are similar. With these similar restaurant combinations, we can output the restaurant recommendation if a user already chooses one.
Based on the above idea, we decide to perform the frequent itemset mining on users’ review data and restaurant list from Yelp to find the combinations of similar restaurants.
Dataset Description
We will use business and review data provided by Yelp which are stored as a collection of JSON format. Each line in data files indicates a record of a restaurant or a review.
For the business data, There are 15 thousands records which contain business data including location data, attributes, and categories about restaurants. For the review data, there are 550 thousand records which contain full review text data including the user_id that wrote the review and the business_id the review is written for.
Business.json: We use the state attribute to filter out the restaurant that is located in Washington state.

## Data Source
Yelp Dataset[1]: https://www.yelp.com/dataset
Business.json example:

    {
        // string, 22 character unique string business id
        "business_id": "tnhfDv5Il8EaGSXZGiuQGg",
    
        // string, the business's name
        "name": "Garaje",
    
        // string, the full address of the business
        "address": "475 3rd St",
    
        // string, the city
        "city": "San Francisco",
    
        // string, 2 character state code, if applicable
        "state": "CA",
    
        // string, the postal code
        "postal code": "94107",
    
        // float, latitude
        "latitude": 37.7817529521,
    
        // float, longitude
        "longitude": -122.39612197,
    
        // float, star rating, rounded to half-stars
        "stars": 4.5,
    
        // integer, number of reviews
        "review_count": 1198,
    
        // integer, 0 or 1 for closed or open, respectively
        "is_open": 1,
    
        // object, business attributes to values. note: some attribute values might be objects
        "attributes": {
            "RestaurantsTakeOut": true,
            "BusinessParking": {
                "garage": false,
                "street": true,
                "validated": false,
                "lot": false,
                "valet": false
            },
        },
    
        // an array of strings of business categories
        "categories": [
            "Mexican",
            "Burgers",
            "Gastropubs"
        ],
    
        // an object of key day to value hours, hours are using a 24hr clock
        "hours": {
            "Monday": "10:00-21:00",
            "Tuesday": "10:00-21:00",
            "Friday": "10:00-21:00",
            "Wednesday": "10:00-21:00",
            "Thursday": "10:00-21:00",
            "Sunday": "11:00-18:00",
            "Saturday": "10:00-21:00"
        }
    }

Review.json: We leverage the user_id and business_id pair to find the frequent itemsets(similar restaurants)
Review.json example:

    {
        // string, 22 character unique review id
        "review_id": "zdSx_SD6obEhz9VrW9uAWA",
    
        // string, 22 character unique user id, maps to the user in user.json
        "user_id": "Ha3iJu77CxlrFm-vQRs_8g",
    
        // string, 22 character business id, maps to business in business.json
        "business_id": "tnhfDv5Il8EaGSXZGiuQGg",
    
        // integer, star rating
        "stars": 4,
    
        // string, date formatted YYYY-MM-DD
        "date": "2016-03-09",
    
        // string, the review itself
        "text": "Great place to hang out after work: the prices are decent, and the ambience is fun. It's a bit loud, but very lively. The staff is friendly, and the food is good. They have a good selection of drinks.",
    
        // integer, number of useful votes received
        "useful": 0,
    
        // integer, number of funny votes received
        "funny": 0,
    
        // integer, number of cool votes received
        "cool": 0
    }

## Proposed Plan
* Main task
    
    Implement a parallel version of the Apriori frequent itemset mining algorithm[2] (major task1) on review data to find similar restaurant combinations.


* Helper task 1
  
    With the help of business data, we will filter the restaurants that are located in Washington state. (There are almost 550 thousand user/business pairs. If we consider all of them, it may cost a couple of days to finish the program. Therefore, we preprocess the whole review data to get a relatively smaller dataset for frequent itemset mining). This preprocess step is supposed to be finished by using Hive[3] (major task2).


* Helper task 2

    If a user only reviews a few restaurants, we consider this user as an unreliable user and filter this user out.


* Potential challenge
    
    We are not familiar with the Hive tool and Apriori algorithm, so it may be hard to implement the whole workflow in time. To solve this problem, we are trying to learn all the needed stuff as early as possible and set an appropriate timeline to control the working process of the project.


## Development

1. Pre-process
    
    Hive

2. A-priori Algorithm

   1. build project with Maven
   2. Run the algorithm with the mapreduce program:
      ```
      java -jar final_project-generator.jar <threshold> <support> <input> <output>
      ```
         1. threshold: Only the users who make comments more than threshold are valid
         2. support: the minimum count to qualify as a frequent itemset
         3. input: the input file with CSV format that contains the information of user id and business id
         4. output: the output file that stores all the qualifies frequent itemsets

## References
[1] Yelp Dataset (https://www.yelp.com/dataset)
[2] Apriori algorithm (https://en.wikipedia.org/wiki/Apriori_algorithm) [3] Apache Hive (https://hive.apache.org/)
