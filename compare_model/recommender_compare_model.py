# has to run in the graphlab environment
from os import path
import graphlab as gl
from datetime import datetime

# Path to the dataset directory
data_dir = './dataset'

# Table of movies we are recommending: busId, name, city, state
items = gl.SFrame.read_csv(path.join(data_dir, 'bus_del_open_new_id.csv'), delimiter='::')

# Table of ratings the users made for items: userId, busId, rating, date
ratings = gl.SFrame.read_csv(path.join(data_dir, 'rating_del_new_id_winter.csv'), delimiter='::')

### Prepare Data ###

# Prepare the data by removing items that are rare
rare_items = ratings.groupby('busId', gl.aggregate.COUNT).sort('Count')
rare_items = rare_items[rare_items['Count'] <= 5]
items = items.filter_by(rare_items['busId'], 'busId', exclude=True)

ratings = ratings[ratings['rating'] >=4 ]
ratings = ratings.filter_by(rare_items['busId'], 'busId', exclude=True)

users = gl.SFrame.read_csv(path.join(data_dir, 'user_names.csv'))

training_data, validation_data = gl.recommender.util.random_split_by_user(ratings, 'userId', 'busId')

### Train Recommender Model ###
model1 = gl.recommender.item_similarity_recommender.create(training_data, 'userId', 'busId', target='rating', similarity_type='jaccard')
model2 = gl.recommender.item_similarity_recommender.create(training_data, 'userId', 'busId', target='rating', similarity_type='pearson')
model3 = gl.recommender.item_similarity_recommender.create(training_data, 'userId', 'busId', target='rating', similarity_type='cosine')
model4 = gl.recommender.factorization_recommender.create(training_data, 'userId', 'busId', target = 'rating')
eval1 = model1.evaluate(validation_data, target='rating')
eval2 = model2.evaluate(validation_data, target='rating')
eval3 = model3.evaluate(validation_data, target='rating')
eval4 = model4.evaluate(validation_data, target='rating')




