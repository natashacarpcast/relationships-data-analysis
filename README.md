# Relationship problems and technology
Final project for MACS 30113 - Big Data and High Performance Computing for Social Scientists \
University of Chicago\
Natasha Carpio Castellanos

## Structure of the project 
- [1-Preprocess-CSV-Upload-to-S3.ipynb](1-Preprocess-CSV-Upload-to-S3.ipynb): This notebook contains code for an initial preprocessing of the CSV file and upload to S3 bucket.
- [2-CSV-to-parquet.ipynb](2-CSV-to-parquet.ipynb): This notebook contains code for converting the CSV into parquet format. 
- [3-DataCleaning-Wrangling.ipynb](3-DataCleaning-Wrangling.ipynb): This notebook removes incomplete data and combines titles and texts of the Reddit’s submissions.
- [4-Topic-Modeling.ipynb](4-Topic-Modeling.ipynb): This notebook performs topic modeling 
- [5-Topic-Modeling-2.ipynb](5-Topic-Modeling-2.ipynb): This notebook categorizes all submissions into topics, and explores samples of submissions from each topic. 
- [6-Sentiment-Analysis.ipynb](6-Sentiment-Analysis.ipynb): This notebook applies VADER Sentiment Analysis to each submission.

## Context

Both theory and empirical studies have shown that social relationships have an impact on health. However, it is not only having relationships that matter, but also the quality of them (Thomas et al., 2022).

Such quality is influenced by social support and relationship strain, among other factors. Depending on the amount of both, the relationship can be helpful or prejudicial for health through different mechanisms (Thomas et al., 2022).

For instance, a supportive relationship can help decrease psychological distress, whereas relationship strain can cause more distress. Ultimately, stress has been linked to multiple health effects, such as cognitive decline. Multiple mechanisms can cause this, such as the higher activation of the hypothalamic-pituitary-adrenal (HPA) axis (Thomas et al., 2022). 

Therefore, it is important to understand what factors could be affecting people’s relationships and taking them into a direction of strain or benefit. 

A relatively new field of research has been studying how social and personal relationships can be affected by communication technologies. On one side, they can help; for instance, it was seen that a higher friendship quality was seen in people who had both offline and online interactions (Scott et al., as cited in Perlman et al., 2021). As for romantic relationships, it was observed that those who were in long-distance relationships got benefits from texting, voice calls, and video calls; as it increased their relationship satisfaction (Holtzman et al., as cited in Perlman et al., 2021). 

However, there’s a “dark side” (Perlman et al., 2021) to this introduction of communication technologies in relationships. For instance, it was seen that “phubbing” (the act of ignoring face-to-face interactions while paying attention to phones) was related to romantic jealousy and attachment anxiety, stress, and depression (David and Roberts, as cited in Perlman et al., 2021). On the other hand, new concepts arise, such as “offline infidelity”, which can create a vague definition and conflict between the involved people (Alexopoulos, as cited in Perlman et al., 2021). 

In sum, communications technologies today are a double-edged sword since, on one hand, they help maintain and engage in relationships, while on the other hand, they can negatively impact them through different mechanisms, such as increasing jealousy and anxiety. 

## Research problem

Although there is already strong evidence on that communication technologies can potentially negatively impact relationships, there is still a lot of ground for more detailed research on the nuances of these issues, such as what proportion they represent in comparison to other types of relationship problems or how the intensity of emotions changes among the different types of problems. 

Although there are many other interesting questions to ask, this project will focus on the initial stages of a larger research idea. Therefore, the specific questions for this moment are: 

1- What topics emerge when people discuss their relationship problems? 

2- Does technology appear in those topics?

3- What proportion of the overall relationship problems is related to communication technologies? 

## Data

Primary data on people’s experiences, such as written stories about their relationships’ issues, are a window into the dynamics of relationship strain and problems. Thanks to Reddit, there’s an immense collection of these stories available for analysis. The subreddit r/relationship_advice has been active for 14 years, where people share their concerns about relationships. 

Data on all submissions posted between 2005 and 2023 are available publicly on Academic Torrents. A 3.52 GB CSV file contained 4,357,389 submissions (raw, dirty data) for analysis. 

## Methodology and computational complexity

To answer the research question, employing topic modeling and sentiment analysis on the data can yield insights into what topics are being discussed on the online forum. 

However, running topic models involve vectorization of all text as well as several iterations on the data set, thus having a time complexity that increases with both the size of the data and other parameters such as the k topics and the number of iterations (Li et al., 2018). Although this can be done in a regular local machine through Python, this could take a long time. As a point of reference, a topic modeling on a CSV of ~36,000 rows took 1 minute 17 seconds to run on a local machine, so it is expected that a topic modeling with this project’s data would take about two hours.

Therefore, scalable computing methods were suggested to parallelize the data processing, get the results faster, and avoid taking up too much memory on a local computer. 

Additionally, it is good practice to keep different versions of the data, such as the raw data and cleaned/preprocessed data (through different steps of the project), and this would have meant having multiple files of ~2-3 GB on a local machine. Depending on a computer’s storage capacity, this could have been more or less feasible, but if one’s in a shared computer at a lab -that also contains data from other projects-, for instance, taking too much storage would not be ideal.

Thus, for this project, AWS was used to optimize both the storage and the time complexity of running the topic model. 

## Scalable computing methods

- Amazon S3 Object storage service: The data was first uploaded programmatically to an S3 bucket from the local machine. After processing the data in multiple steps, more versions were stored in the same bucket (Cleaned data and data with new features). As mentioned above, this service helped optimize storage. 

- Apache Parquet: A column-oriented way of storing data that provided high performance compression to minimize size of files and optimize storage (Apache Parquet, 2024). 

- Amazon EMR Cluster Platform: Through this cloud computing solution, I was able to request multiple EC2 instances. For the preprocessing and cleaning of the data, I used 3 general purpose m5.xlarge instances, each containing 4 vCPUs and thus allowing to parallelize the preprocessing of the data. Later, for the topic modeling and sentiment analysis, 8 r5.xlarge instances were requested, which are memory-optimized and help with big data analytics (Amazon Web Services, 2024) 

- Apache Spark and Spark NLP: These solutions, integrated in the EMR cluster, allowed for the parallelizable use of functions and methods.

## Results and Limitations

As for RQ 1, the topic modeling yielded some interesting topics but also unclear ones. Among those that were easier to interpret, it was observed that there are two types of relationships that cause concern: romantic and family relationships. Concerns about romantic relationships can be divided into two: of the dating stage and of the “official” relationship stage.

Furthermore, different types of problems were observed. One group of problems concerns finances, employment, and housing, whereas another concerns social media and messaging (thus answering RQ 2). 

Among all the subreddit’s data, 98,879 submissions are suggested to be related to social media, messaging, or technology in general (6% of all the data), answering RQ 3. 

Further work will build on these results and evaluate changes in topics over the years, as well as differences in engagement through the number of comments and scores (operationalized as upvotes minus downvotes). 

Although sentiment analysis was applied, a comparison between topics and among years was not completed, so it will remain part of future work. 

One significant limitation of these findings is that, despite utilizing the EMR cluster, the computation time was excessively long. Topic modeling required approximately 50 minutes, which is about 50% less than it would have taken on a single node, but still not fast enough to make it practical to experiment with different parameters, analyze fit statistics, and optimize the model. Additionally, saving the results to new parquet files took an extra 40 minutes.

Future work, with more resources (either more time availability or EC2 instances), will try different parameters and compare performance metrics such as perplexity and semantic coherence to choose the best model and achieve more reliable results.
 
## Sources 

- Amazon Web Services. (2024). Amazon EC2. https://aws.amazon.com/ec2/
- Apache Parquet. (2024) Overview. https://parquet.apache.org/docs/overview/
- Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.
- Li, X., Li, C., Chi, J., & Ouyang, J. (2018). Short text topic modeling by exploring original documents. Knowledge and Information Systems, 56(2), 443–462. https://doi.org/10.1007/s10115-017-1099-0
- Perlman, D., Sprecher, S., & Drouin, M. (2021). Introduction to the special issue on communication technologies and relationships. Journal of Social and Personal Relationships, 38, 026540752110609. https://doi.org/10.1177/02654075211060905
- Thomas, P. A., Williams-Farrelly, M. M., Sauerteig, M. R., & Ferraro, K. F. (2022). Childhood Stressors, Relationship Quality, and Cognitive Health in Later Life. The journals of gerontology. Series B, Psychological sciences and social sciences, 77(7), 1361–1371. https://doi.org/10.1093/geronb/gbac007




