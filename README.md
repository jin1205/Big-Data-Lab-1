# Big-Data-Lab-1

•	Built a personalized stores recommendation system, implemented with Spark, which recommended stores to new users for different seasons and cities

•	Compared the four recommendation models, which are implemented by the Graphlab library, which discovered that the matrix factorization collaborative filtering model produced the best performance

•	Designed a web frontend, using the Django as web framework and Celery, which sent the task to the spark job and the result back to the frontend

To run the code:

•	Command on the cluster :
We uploaded a python script task_recommend.py under /user/yiles/ and our related library : spark_celery is on the same path; the command to run our spark job is as below: 

spark-submit --master=yarn-client task_recommend.py

•	Run the python script to start the front end, our python script is under /web/manage.py

python manage.py runserver

•	Access the front end :
http://localhost:/8000/recommend

if the default port 8000 of your PC is occupied, then you can use alternative port:
python manage.py runserver 8080 
