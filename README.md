Context

Both theory and empirical studies have shown that social relationships have an impact for health. However, it is not only having relationships that matter, but also the quality of these relationships (Thomas et. Al, 2022).

The quality is influenced by social support and relationship strain, among other factors. Depending on the amount of both, the relationship can be advantegous or perjudicial for health through different mechanisms (Thomas et. Al, 2022).

For instance, a supportive relationship can help with decreassing psychological distress, whereas raltionship strain can cause more distress. Ultimately, stress has been linked to multipl health effects such as cognitive decline. There are multiple mechanisms through which these can happen, such as how people can find unhealthy ways of coping with stress or even how it can be associated with detrimental physical reactions such as the higher activation of the hypothalamic - pituitary-adrenal (HPA) axis (Thomas et. Al, 2022). 

Therefore, it is important to understand what factors that could be affecting people’s relationship and taking them into a direction of strain or a good direction of support. 

A relatively new field of research has been studying how social and personal relationships can be affected by communication technologies. On one side, they can help. For instance, it was seen that highest friendship quality was seen in people who had both offline and online interactions (Scott et al., as cited in Perlman et al., 2021). As for romantic relationships, it was seen that those who were in long-distance relationships saw benefits from texting, voice and videocalls ad it increased their relationship satisfaction (Holtzman et al., as cited in Perlman et al., 2021). 

However, there’s a “dark side” (Perlman et al., 2021) to this introduction of communication technologies in relationships. For instance it was seen that “phubbing” (the act of ignoring face-to-face interactions while paying attention to phones) was related to romantic jealously and attachment anxiety, stress and depression (David and Roberts, as cited in Perlman et al., 2021). On the other hand, new concepts arise such as “offline infidelity” which then creates a vague definition and conflict between the actors, where one person of a couple may feel betrayed while the other negates such infidelity by justifying it was an online interaction. (Alexopoulos, as cited in Perlman et al., 2021). 

In sum, communications technologies now a days are a double-sword aspect since on one side they help maintain and engage in relationships, while on the other side they can negatively impact them by different mechanisms such as increasing jealously and anxiety. 

Research problem

Although there is already a strong evidence on the fact that communication tehcnologies can impact relationships negatively, there is still a lot of ground for more detailed research on the nuances of this issues. Some research questions are:
1- Throughout the years, Are these type of problems gaining more relevance in comparison to other type of problems?  In other words, to what extent is this aspect playing a small or large role in relationship strain?
2 -Do these problems affect different types of relationships equally (kin relationships, friendships and romantic relationships) or do they play a larger role in a certain type of relation? 
3- What specific type of technologies affect more? For instance, the use of social media? The availability for texting, etc?

Many questions can be asked, but for the purposes of this project the focus will be on identifying the main concerns people share when asking about relationship advice and determining what role do technology play. 

The main researchs question are: 

1- What topics are people discussing when talking about their relationship problems?

1- What proportion of the overall relationship problems is related to communication technologies? 

Data

Primary data on people’s experiences, such as their written stories about their relationships’ issues, are a window into these dynamics of relationship strain and problems. Thanks to Reddit, there’s an immense collection of these stories available for analysis. The subreddit r/relationship_advice has been active for 14 years, where people  share there concerns about relationships. 

Data on all the submissions between 2005 and 2023 are available publicly on Academic Torrents. In total 4,357,389 submissions (raw, dirty data) were available for analysis on a 3.52 GB CSV file. 

Methodology and computational complexity

To answer the research question, employing topic modeling and sentiment analysis on the data can yield insights on what topics are being discussed on the online forum. 

However, running topic models involve vectorization of all text as well as several iterations on the data set, thus having a time complexity that increases with both the size of the data and other parameters such as the k topics and the number of iterations (Li et al., 2018). Although this can be done in a regular local machine through Python, a long time of running the model is expected. As a point of reference, a topic modeling on a CSV of ~36,000 rows took 1 min 17 seconds to run in my local machine, so I was expecting about two hours for running a topic modeling on the data for this project. 

Therefore, using scalable computing methods was suggested that could help with parallelizing the processing of the data for getting the results faster and without taking too much memory of the local computer. 

Aditionally, it is good practice to keep different versions of the data such as raw data, cleaned/preprocessed data (through different steps of the project) and this would have meant having multiple files of ~2-3 GB on a local machine. Depending on a computer’s storage capacity this could have been more or less feasible, but if one’s in a shared computer at a lab which also contains data of other projects, for instance, taking so much storage resources would not be ideal.

Thus, for this project the AWS services were used to optimize both the storage and the time complexity of running the topic model. 



Scalable computing methods

Amazon S3 - Object storage service: The data was first uploaded to an S3 bucket programmatically from the local machine. [(1-Preprocess-CSV-Upload-to-S3.ipynb)](1-Preprocess-CSV-Upload-to-S3.ipynb). After processing the data in multiple steps, more versions were stored in the same bucket (Cleaned data and data with new features). As mentioned above, this service helped for optimizing storage. 

Apache Parquet: This is a column-oriented way of storing data that provides high performance compression to minimize size of files and optimize storage (Apache Parquet, 2024). 

Amazon EMR Cluster Platform: Through this cloud computing solution, I was able to request multiple EC2 instances. For the preprocessing and cleaning of the data I used 3 general purpose m5.xlarge instances, which each of them contained 4 vCPUs and thus allowing me to parallelize the processing of the data. Later, for the topic modeling and sentiment analysis, 8 r5.xlarge instances were requested, which are memory optimized and help with big data analytics (Amazon Web Services, 2024) 

Apache Spark and Spark NLP: These solutions, integrated in the EMR cluster, allowed for the usage of functions and methods in a parallelizable way. 


Structure of the project 

[1-Preprocess-CSV-Upload-to-S3.ipynb](1-Preprocess-CSV-Upload-to-S3.ipynb): This notebook contains code for an initial preprocessing of the CSV file and upload to S3 bucket.
[2-CSV-to-parquet.ipynb](2-CSV-to-parquet.ipynb): This notebook contains code for converting the CSV into parquet format. 
[3-DataCleaning-Wrangling.ipynb](3-DataCleaning-Wrangling.ipynb): This notebook removes incomplete data and combines titles and texts of the Reddit’s submissions.
[4-Topic-Modeling.ipynb](4-Topic-Modeling.ipynb): This notebook performs topic modeling 
[5-Topic-Modeling-2.ipynb](5-Topic-Modeling-2.ipynb): This notebook categorizes all submissions into topics, and explores samples of submissions from each topic. 
[6-Sentiment-Analysis.ipynb](6-Sentiment-Analysis.ipynb): This notebook applies VADER Sentiment Analysis to each submission. 

Results and Limitations

The topic modeling yield some interesting topics but also unclear ones. Among those which were easier to interpret, it was observed that there are two types of relationships that cause concern: romantic and family relationships. Concerns about romantic relationship can be divided into two: for the dating stage, and for a more long-term/official relationship stage.

Aditionally, different types of problems were observed. One group of problems relate to finances, employment and housing, whereas another does relate to social media and messaging (and thus relevant for the research questions). 

Among all the subreddit’s data, 98,879 are hypothesized to be related to social media, messaging or technology in general (6% of all the data). 

Further work will build up on evaluating changes in topics over the years, as well as differences in engagement through number of comments and score (operationalized as upvotes - downvotes). 

Although sentiment analysis was applied, comparison between topics and among years was not completed, so it will remain as part of future work. 

A major limitation for these results is that, although using the EMR cluster, computation time was very long. The topic modeling took about 50 minutes, at least 50% less than it would have taken on a single node, but still not fast enough for it to be feasible to try different parameters and fine tune the model for its best performance. Saving the results to new parquet files took around 40 minutes more. 

Future work, with more resources (either more time availability or EC2 instances) will try different parameters and compare performance metrics such as perplexity and semantic coherence to choose the best model and achieve more valid results.






## Sources 

Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.

Thomas, P. A., Williams-Farrelly, M. M., Sauerteig, M. R., & Ferraro, K. F. (2022). Childhood Stressors, Relationship Quality, and Cognitive Health in Later Life. The journals of gerontology. Series B, Psychological sciences and social sciences, 77(7), 1361–1371. https://doi.org/10.1093/geronb/gbac007

Perlman, D., Sprecher, S., & Drouin, M. (2021). Introduction to the special issue on communication technologies and relationships. Journal of Social and Personal Relationships, 38, 026540752110609. https://doi.org/10.1177/02654075211060905

Li, X., Li, C., Chi, J., & Ouyang, J. (2018). Short text topic modeling by exploring original documents. Knowledge and Information Systems, 56(2), 443–462. https://doi.org/10.1007/s10115-017-1099-0

Amazon Web Services. (2024). Amazon EC2. https://aws.amazon.com/ec2/

Apache Parquet. (2024) Overview. https://parquet.apache.org/docs/overview/
